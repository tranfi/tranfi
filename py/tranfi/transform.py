"""Prepared, reusable typed transforms backed by Tranfi's C engine."""

from array import array
import ctypes
from dataclasses import dataclass
import sys

from . import _ffi


TF_VIEW_FLOAT32 = 0x0101
TF_VIEW_FLOAT64 = 0x0102
TF_TRANSFORM_SCHEMA_INPUT = 1
TF_TRANSFORM_SCHEMA_OUTPUT = 2

_DTYPE_CODES = {
    'float32': TF_VIEW_FLOAT32,
    'float64': TF_VIEW_FLOAT64,
}
_DTYPE_FORMATS = {
    TF_VIEW_FLOAT32: ('f', 4),
    TF_VIEW_FLOAT64: ('d', 8),
}
_LIMIT_FIELDS = tuple(
    name for name, _ in _ffi._TfTransformLimitsV1._fields_
    if name not in ('abi_version', 'struct_size')
)
_SIZE_MAX = ctypes.c_size_t(-1).value
_CANCEL_BYTES = 65536
_CANCEL_ITERS = 4096


class TranfiTransformError(RuntimeError):
    """Stable prepared-transform error with the numeric C error code."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = int(code)


class _TransformCancelState(ctypes.Structure):
    _fields_ = [
        ('requested', ctypes.c_int),
        ('poll_count', ctypes.c_uint64),
    ]


@_ffi._TRANSFORM_CANCEL_CALLBACK
def _poll_cancel_flag(user):
    if not user:
        return 0
    state = ctypes.cast(user, ctypes.POINTER(_TransformCancelState)).contents
    state.poll_count += 1
    return 1 if state.requested else 0


class TransformCancelToken:
    """Thread-safe one-way cancellation flag retained by active sessions."""

    def __init__(self):
        self._state = _TransformCancelState()

    @property
    def requested(self):
        return bool(self._state.requested)

    @property
    def _poll_count(self):
        return int(self._state.poll_count)

    def request(self):
        self._state.requested = 1

    def _poll(self):
        self._state.poll_count += 1
        return bool(self._state.requested)


class TransformLimits:
    """C-owned safe resource profile with explicit field overrides."""

    def __init__(self, **overrides):
        self._c = _ffi._TfTransformLimitsV1()
        lib = _ffi._load_lib()
        code = lib.tf_transform_limits_init_safe_v1(
            ctypes.byref(self._c), ctypes.sizeof(self._c))
        if code != 0:
            raise TranfiTransformError(code, 'failed to initialize transform limits')
        unknown = set(overrides) - set(_LIMIT_FIELDS)
        if unknown:
            names = ', '.join(sorted(unknown))
            raise TypeError(f'unknown transform limit field(s): {names}')
        for name, value in overrides.items():
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise TranfiTransformError(
                    100, f'{name} must be a positive integer')
            if value > (1 << 64) - 1:
                raise OverflowError(f'{name} exceeds uint64')
            setattr(self._c, name, value)

    def to_dict(self):
        return {name: int(getattr(self._c, name)) for name in _LIMIT_FIELDS}

    def __getattr__(self, name):
        if name in _LIMIT_FIELDS:
            return int(getattr(self._c, name))
        raise AttributeError(name)


@dataclass(frozen=True)
class DenseResult:
    rows: int
    columns: int
    data: array
    dtype: str = 'float64'


def _limits_pointer(limits):
    if limits is None:
        return None
    if not isinstance(limits, TransformLimits):
        raise TypeError('limits must be TransformLimits or None')
    return ctypes.byref(limits._c)


def _resolved_limits(limits):
    if limits is None:
        return TransformLimits()
    _limits_pointer(limits)
    return limits


def _runtime(limits, cancel_token=None):
    if limits is None and cancel_token is None:
        return None, None
    if limits is not None:
        _limits_pointer(limits)
    if cancel_token is not None and not isinstance(
            cancel_token, TransformCancelToken):
        raise TypeError('cancel_token must be TransformCancelToken or None')
    runtime = _ffi._TfTransformRuntimeV1()
    runtime.abi_version = 1
    runtime.struct_size = ctypes.sizeof(runtime)
    if limits is not None:
        runtime.limits = ctypes.pointer(limits._c)
    if cancel_token is not None:
        runtime.cancel = ctypes.cast(
            _poll_cancel_flag, ctypes.c_void_p).value
        runtime.cancel_user = ctypes.addressof(cancel_token._state)
    return runtime, ctypes.byref(runtime)


def _resource_limit(message):
    raise TranfiTransformError(104, message)


def _raise_if_cancelled(cancel_token, owner=None):
    if cancel_token is not None and cancel_token._poll():
        if owner is not None:
            owner.close()
        raise TranfiTransformError(109, 'prepared transform cancelled')


def _size_t(value, name, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TypeError(f'{name} must be a nonnegative integer')
    if positive and value == 0:
        raise TypeError(f'{name} must be positive')
    if value > _SIZE_MAX:
        raise OverflowError(f'{name} exceeds size_t')
    return value


def _destroy_error(error):
    if error.value:
        _ffi._load_lib().tf_transform_error_destroy(ctypes.byref(error))


def _raise_transform(code, error):
    lib = _ffi._load_lib()
    message = f'prepared transform failed with code {int(code)}'
    try:
        if error.value:
            length = ctypes.c_size_t()
            pointer = lib.tf_transform_error_message(
                error, ctypes.byref(length))
            if pointer:
                raw = ctypes.string_at(pointer, length.value)
                message = raw.decode('utf-8', errors='replace')
    finally:
        _destroy_error(error)
    raise TranfiTransformError(code, message)


def _check(code, error):
    if code != 0:
        _raise_transform(code, error)
    if error.value:
        _destroy_error(error)
        raise TranfiTransformError(199, 'C returned success with an error object')


def _input_bytes(value, name, *, max_bytes=None, cancel_token=None):
    if isinstance(value, str):
        if max_bytes is not None and len(value) > max_bytes:
            _resource_limit(f'{name} exceeds its byte limit')
        value = value.encode('utf-8')
    try:
        view = memoryview(value)
    except TypeError as exc:
        raise TypeError(f'{name} must be str or a bytes-like object') from exc
    if not view.c_contiguous:
        raise ValueError(f'{name} must be C-contiguous')
    if view.nbytes > _SIZE_MAX:
        raise OverflowError(f'{name} exceeds size_t')
    if max_bytes is not None and view.nbytes > max_bytes:
        _resource_limit(f'{name} exceeds its byte limit')
    byte_view = view.cast('B')
    _raise_if_cancelled(cancel_token)
    owner = (ctypes.c_uint8 * byte_view.nbytes)()
    pointer = ctypes.addressof(owner) if byte_view.nbytes else None
    for offset in range(0, byte_view.nbytes, _CANCEL_BYTES):
        _raise_if_cancelled(cancel_token)
        end = min(offset + _CANCEL_BYTES, byte_view.nbytes)
        chunk = byte_view[offset:end].tobytes()
        ctypes.memmove(pointer + offset, chunk, len(chunk))
    _raise_if_cancelled(cancel_token)
    typed_pointer = (ctypes.cast(pointer, ctypes.POINTER(ctypes.c_uint8))
                     if pointer is not None else None)
    return owner, typed_pointer, byte_view.nbytes


def _utf8_chunks(value, label, cancel_token):
    if not isinstance(value, str) or not value:
        raise TypeError(f'{label} must be a nonempty string')
    chunks = []
    total = 0
    chunk_chars = min(_CANCEL_ITERS, _CANCEL_BYTES // 4)
    for offset in range(0, len(value), chunk_chars):
        _raise_if_cancelled(cancel_token)
        chunk = value[offset:offset + chunk_chars].encode('utf-8')
        if b'\0' in chunk:
            raise TypeError(f'{label} must not contain NUL')
        chunks.append(chunk)
        total += len(chunk)
    _raise_if_cancelled(cancel_token)
    return chunks, total


def _chunks_owner(chunks, total, cancel_token):
    _raise_if_cancelled(cancel_token)
    owner = (ctypes.c_uint8 * total)()
    pointer = ctypes.addressof(owner)
    offset = 0
    for chunk in chunks:
        _raise_if_cancelled(cancel_token)
        ctypes.memmove(pointer + offset, chunk, len(chunk))
        offset += len(chunk)
    _raise_if_cancelled(cancel_token)
    return owner


class _SchemaOwner:
    def __init__(self, fields, limits, cancel_token=None):
        if not isinstance(fields, (list, tuple)) or not fields:
            raise TypeError('schema must be a nonempty list of field mappings')
        if len(fields) > limits.max_input_columns:
            _resource_limit('prepared-transform input column limit exceeded')
        fields_bytes = len(fields) * ctypes.sizeof(_ffi._TfFieldViewV1)
        if fields_bytes > _SIZE_MAX:
            raise OverflowError('schema descriptor bytes exceed size_t')
        if fields_bytes > limits.max_allocation_bytes:
            _resource_limit('prepared-transform schema allocation limit exceeded')
        self.dtypes = []
        self._strings = []
        prepared = []
        decoded_bytes = 0
        for index, field in enumerate(fields):
            if index % _CANCEL_ITERS == 0:
                _raise_if_cancelled(cancel_token)
            if not isinstance(field, dict):
                raise TypeError(f'schema field {index} must be a mapping')
            identifier = field.get('id')
            name = field.get('name', identifier)
            dtype = field.get('dtype')
            if not isinstance(identifier, str) or not identifier:
                raise TypeError(
                    f'schema field {index} id must be a nonempty string')
            if not isinstance(name, str) or not name:
                raise TypeError(
                    f'schema field {index} name must be a nonempty string')
            if (len(identifier) > limits.max_string_bytes
                    or len(name) > limits.max_string_bytes):
                _resource_limit(
                    'prepared-transform schema string limit exceeded')
            id_chunks, id_len = _utf8_chunks(
                identifier, f'schema field {index} id', cancel_token)
            name_chunks, name_len = _utf8_chunks(
                name, f'schema field {index} name', cancel_token)
            if dtype not in _DTYPE_CODES:
                raise TypeError(
                    f'schema field {index} dtype must be float32 or float64')
            if (id_len > limits.max_string_bytes
                    or name_len > limits.max_string_bytes
                    or id_len > limits.max_allocation_bytes
                    or name_len > limits.max_allocation_bytes):
                _resource_limit(
                    'prepared-transform schema string limit exceeded')
            decoded_bytes += id_len + name_len
            if decoded_bytes > limits.max_decoded_string_bytes:
                _resource_limit(
                    'prepared-transform decoded string limit exceeded')
            prepared.append((dtype, id_chunks, id_len, name_chunks, name_len))

        _raise_if_cancelled(cancel_token)
        self._fields = (_ffi._TfFieldViewV1 * len(fields))()
        for index, (dtype, id_chunks, id_len,
                    name_chunks, name_len) in enumerate(prepared):
            if index % _CANCEL_ITERS == 0:
                _raise_if_cancelled(cancel_token)
            id_owner = _chunks_owner(id_chunks, id_len, cancel_token)
            name_owner = _chunks_owner(name_chunks, name_len, cancel_token)
            self._strings.extend((id_owner, name_owner))
            view = self._fields[index]
            view.abi_version = 1
            view.struct_size = ctypes.sizeof(view)
            view.dtype = _DTYPE_CODES[dtype]
            view.id_utf8 = ctypes.cast(
                id_owner, ctypes.POINTER(ctypes.c_uint8))
            view.id_bytes = id_len
            view.name_utf8 = ctypes.cast(
                name_owner, ctypes.POINTER(ctypes.c_uint8))
            view.name_bytes = name_len
            self.dtypes.append(_DTYPE_CODES[dtype])
        self.view = _ffi._TfSchemaViewV1()
        self.view.abi_version = 1
        self.view.struct_size = ctypes.sizeof(self.view)
        self.view.column_count = len(fields)
        self.view.fields = ctypes.cast(
            self._fields, ctypes.POINTER(_ffi._TfFieldViewV1))
        self.view.fields_bytes = fields_bytes


def _buffer_views(value, name, *, byte_buffer=False):
    try:
        view = memoryview(value)
    except TypeError as exc:
        raise TypeError(f'{name} must support the buffer protocol') from exc
    if view.ndim != 1 or not view.c_contiguous:
        raise ValueError(f'{name} must be a one-dimensional C-contiguous buffer')
    if byte_buffer:
        try:
            byte_view = view.cast('B')
        except (TypeError, ValueError) as exc:
            raise TypeError(f'{name} must be byte-addressable') from exc
    else:
        byte_view = view.cast('B')
    if byte_view.nbytes > _SIZE_MAX:
        raise OverflowError(f'{name} exceeds size_t')
    return view, byte_view


def _buffer_pointer(
        view, byte_view, required_bytes, cancel_token=None):
    if required_bytes == 0:
        return view, byte_view, None, None, 0
    if byte_view.readonly:
        _raise_if_cancelled(cancel_token)
        owner = (ctypes.c_uint8 * required_bytes)()
        pointer = ctypes.addressof(owner)
        for offset in range(0, required_bytes, _CANCEL_BYTES):
            _raise_if_cancelled(cancel_token)
            end = min(offset + _CANCEL_BYTES, required_bytes)
            chunk = byte_view[offset:end].tobytes()
            ctypes.memmove(pointer + offset, chunk, len(chunk))
        _raise_if_cancelled(cancel_token)
        return view, byte_view, owner, pointer, required_bytes
    first = ctypes.c_uint8.from_buffer(byte_view)
    pointer = ctypes.addressof(first)
    return view, byte_view, first, pointer, required_bytes


class _TableOwner:
    def __init__(
            self, table, expected_dtypes, limits, phase,
            cancel_token=None):
        if not isinstance(table, dict):
            raise TypeError('table must be a mapping with rows and columns')
        rows = _size_t(table.get('rows'), 'table rows')
        columns = table.get('columns')
        if not isinstance(columns, (list, tuple)):
            raise TypeError('table columns must be a list')
        if len(columns) != len(expected_dtypes):
            raise ValueError('table column count does not match the fitted schema')
        max_rows = (limits.max_analyzer_rows if phase == 'analyze'
                    else limits.max_apply_rows)
        max_input_bytes = (
            limits.max_analyzer_input_bytes if phase == 'analyze'
            else limits.max_apply_input_bytes)
        if rows > max_rows:
            _resource_limit('prepared-transform row limit exceeded')
        columns_bytes = len(columns) * ctypes.sizeof(_ffi._TfColumnViewV1)
        if columns_bytes > _SIZE_MAX:
            raise OverflowError('table descriptor bytes exceed size_t')
        if columns_bytes > limits.max_allocation_bytes:
            _resource_limit(
                'prepared-transform table allocation limit exceeded')

        prepared = []
        total_input_bytes = 0
        for index, (column_value, dtype) in enumerate(
                zip(columns, expected_dtypes)):
            if index % _CANCEL_ITERS == 0:
                _raise_if_cancelled(cancel_token)
            config = (column_value if isinstance(column_value, dict)
                      else {'data': column_value})
            if 'data' not in config:
                raise TypeError(f'table column {index} is missing data')
            data_view, byte_view = _buffer_views(
                config['data'], f'table column {index} data')
            if (rows == 0 and (byte_view.nbytes != 0
                    or config.get('validity') is not None
                    or 'validity_bit_offset' in config
                    or 'validity_bit_stride' in config)):
                raise TranfiTransformError(
                    100,
                    'zero-row column must carry empty data and no validity span')
            expected_format, item_size = _DTYPE_FORMATS[dtype]
            actual_format = data_view.format
            if actual_format and actual_format[0] in '@=':
                actual_format = actual_format[1:]
            elif actual_format and actual_format[0] == '<':
                if sys.byteorder != 'little':
                    raise TypeError(
                        f'table column {index} must use native-endian values')
                actual_format = actual_format[1:]
            elif actual_format and actual_format[0] in '>!':
                if sys.byteorder != 'big':
                    raise TypeError(
                        f'table column {index} must use native-endian values')
                actual_format = actual_format[1:]
            if actual_format != expected_format or data_view.itemsize != item_size:
                expected_name = (
                    'float32' if dtype == TF_VIEW_FLOAT32 else 'float64')
                raise TypeError(
                    f'table column {index} must expose native {expected_name} values')
            stride = _size_t(
                config.get('stride_bytes', item_size),
                f'table column {index} stride_bytes', positive=True)
            data_bytes = 0 if rows == 0 else (rows - 1) * stride + item_size
            if data_bytes > _SIZE_MAX:
                raise OverflowError(
                    f'table column {index} data span exceeds size_t')
            if data_bytes > byte_view.nbytes:
                raise ValueError(f'table column {index} data buffer is too small')
            if data_bytes > limits.max_allocation_bytes:
                _resource_limit(
                    'prepared-transform input allocation limit exceeded')

            validity_view = validity_byte_view = None
            validity_bytes = 0
            validity = config.get('validity')
            if validity is not None:
                validity_view, validity_byte_view = _buffer_views(
                    validity, f'table column {index} validity',
                    byte_buffer=True)
                bit_offset = _size_t(
                    config.get('validity_bit_offset', 0),
                    f'table column {index} validity_bit_offset')
                bit_stride = _size_t(
                    config.get('validity_bit_stride', 1),
                    f'table column {index} validity_bit_stride', positive=True)
                required_bits = (0 if rows == 0 else
                                 bit_offset + (rows - 1) * bit_stride + 1)
                if required_bits > _SIZE_MAX:
                    raise OverflowError(
                        f'table column {index} validity span exceeds size_t')
                validity_bytes = (required_bits + 7) // 8
                if validity_bytes > validity_byte_view.nbytes:
                    raise ValueError(
                        f'table column {index} validity buffer is too small')
                if validity_bytes > limits.max_allocation_bytes:
                    _resource_limit(
                        'prepared-transform validity allocation limit exceeded')
            else:
                if ('validity_bit_offset' in config
                        or 'validity_bit_stride' in config):
                    raise TypeError(
                        f'table column {index} validity bit metadata requires validity')
                bit_offset = 0
                bit_stride = 0

            total_input_bytes += data_bytes + validity_bytes
            if total_input_bytes > max_input_bytes:
                _resource_limit('prepared-transform input byte limit exceeded')
            prepared.append({
                'data_view': data_view,
                'byte_view': byte_view,
                'data_bytes': data_bytes,
                'stride': stride,
                'validity_view': validity_view,
                'validity_byte_view': validity_byte_view,
                'validity_bytes': validity_bytes,
                'bit_offset': bit_offset,
                'bit_stride': bit_stride,
            })

        self._owners = []
        _raise_if_cancelled(cancel_token)
        self._columns = (_ffi._TfColumnViewV1 * len(columns))()
        for index, item in enumerate(prepared):
            if index % _CANCEL_ITERS == 0:
                _raise_if_cancelled(cancel_token)
            data_parts = _buffer_pointer(
                item['data_view'], item['byte_view'], item['data_bytes'],
                cancel_token)
            self._owners.extend(data_parts[:3])
            pointer = data_parts[3]
            validity_pointer = None
            if item['validity_view'] is not None:
                validity_parts = _buffer_pointer(
                    item['validity_view'], item['validity_byte_view'],
                    item['validity_bytes'], cancel_token)
                self._owners.extend(validity_parts[:3])
                validity_pointer = validity_parts[3]
            view = self._columns[index]
            view.abi_version = 1
            view.struct_size = ctypes.sizeof(view)
            view.data = pointer
            view.data_bytes = item['data_bytes']
            view.stride_bytes = item['stride']
            if validity_pointer is not None:
                view.validity = ctypes.cast(
                    validity_pointer, ctypes.POINTER(ctypes.c_uint8))
            view.validity_bytes = item['validity_bytes']
            view.validity_bit_offset = item['bit_offset']
            view.validity_bit_stride = item['bit_stride']
        self.view = _ffi._TfTableViewV1()
        self.view.abi_version = 1
        self.view.struct_size = ctypes.sizeof(self.view)
        self.view.row_count = rows
        self.view.column_count = len(columns)
        self.view.columns = ctypes.cast(
            self._columns, ctypes.POINTER(_ffi._TfColumnViewV1))
        self.view.columns_bytes = columns_bytes


class _OwnedHandle:
    _destroy_name = None

    def __new__(cls, *args, **kwargs):
        del args, kwargs
        instance = super().__new__(cls)
        # A subclass constructor can fail before _OwnedHandle.__init__ runs.
        # Keep the inherited finalizer safe for that partially initialized
        # object; ownership of a transferred non-null handle remains with
        # _adopt_transferred_handle until construction succeeds.
        instance._handle = ctypes.c_void_p()
        return instance

    def __init__(self, handle):
        self._handle = (handle if isinstance(handle, ctypes.c_void_p)
                        else ctypes.c_void_p(handle))

    def _require_open(self):
        if not self._handle.value:
            raise TranfiTransformError(112, 'prepared transform object is closed')
        return self._handle

    def close(self):
        if self._handle.value:
            destroy = getattr(_ffi._load_lib(), self._destroy_name)
            destroy(ctypes.byref(self._handle))

    def __enter__(self):
        self._require_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type, exc_value, traceback
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


def _adopt_transferred_handle(handle, destroy_name, constructor, *args):
    try:
        return constructor(handle, *args)
    except BaseException:
        if handle.value:
            destroy = getattr(_ffi._load_lib(), destroy_name)
            destroy(ctypes.byref(handle))
        raise


class TransformRecipe(_OwnedHandle):
    _destroy_name = 'tf_transform_recipe_destroy'

    @classmethod
    def from_json(cls, recipe_json, *, limits=None):
        effective_limits = _resolved_limits(limits)
        max_bytes = min(
            effective_limits.max_recipe_bytes,
            effective_limits.max_allocation_bytes)
        owner, pointer, length = _input_bytes(
            recipe_json, 'recipe_json', max_bytes=max_bytes)
        handle = ctypes.c_void_p()
        error = ctypes.c_void_p()
        code = _ffi._load_lib().tf_transform_recipe_from_json(
            pointer, length, _limits_pointer(effective_limits),
            ctypes.byref(handle), ctypes.byref(error))
        del owner
        _check(code, error)
        return _adopt_transferred_handle(
            handle, 'tf_transform_recipe_destroy', cls)

    def analyzer(self, schema, *, limits=None, cancel_token=None):
        effective_limits = _resolved_limits(limits)
        runtime, runtime_pointer = _runtime(
            effective_limits, cancel_token)
        _raise_if_cancelled(cancel_token)
        schema_owner = _SchemaOwner(
            schema, effective_limits, cancel_token)
        _raise_if_cancelled(cancel_token)
        handle = ctypes.c_void_p()
        error = ctypes.c_void_p()
        code = _ffi._load_lib().tf_transform_analyzer_create(
            self._require_open(), ctypes.byref(schema_owner.view),
            runtime_pointer, ctypes.byref(handle), ctypes.byref(error))
        del runtime
        _check(code, error)
        return _adopt_transferred_handle(
            handle, 'tf_transform_analyzer_destroy', TransformAnalyzer,
            schema_owner.dtypes, cancel_token, effective_limits)


class TransformAnalyzer(_OwnedHandle):
    _destroy_name = 'tf_transform_analyzer_destroy'

    def __init__(self, handle, dtypes, cancel_token=None, limits=None):
        super().__init__(handle)
        self._dtypes = tuple(dtypes)
        self._cancel_token = cancel_token
        self._limits = limits

    def push(self, table):
        self._require_open()
        _raise_if_cancelled(self._cancel_token, self)
        try:
            owner = _TableOwner(
                table, self._dtypes, self._limits, 'analyze',
                self._cancel_token)
        except TranfiTransformError as exc:
            if exc.code == 109:
                self.close()
            raise
        _raise_if_cancelled(self._cancel_token, self)
        error = ctypes.c_void_p()
        code = _ffi._load_lib().tf_transform_analyzer_push(
            self._require_open(), ctypes.byref(owner.view), ctypes.byref(error))
        if code == 109:
            self.close()
        _check(code, error)
        return self

    def finalize(self):
        handle = ctypes.c_void_p()
        error = ctypes.c_void_p()
        code = _ffi._load_lib().tf_transform_analyzer_finalize(
            self._require_open(), ctypes.byref(handle), ctypes.byref(error))
        if code == 109:
            self.close()
        _check(code, error)
        return _adopt_transferred_handle(
            handle, 'tf_transform_plan_destroy', TransformPlan)


class TransformPlan(_OwnedHandle):
    _destroy_name = 'tf_transform_plan_destroy'

    @classmethod
    def from_bytes(cls, data, *, limits=None, cancel_token=None):
        if isinstance(data, str):
            raise TypeError('plan bytes must be a bytes-like object')
        effective_limits = _resolved_limits(limits)
        runtime, runtime_pointer = _runtime(
            effective_limits, cancel_token)
        _raise_if_cancelled(cancel_token)
        max_bytes = min(
            effective_limits.max_plan_bytes,
            effective_limits.max_allocation_bytes)
        owner, pointer, length = _input_bytes(
            data, 'plan bytes', max_bytes=max_bytes,
            cancel_token=cancel_token)
        _raise_if_cancelled(cancel_token)
        handle = ctypes.c_void_p()
        error = ctypes.c_void_p()
        code = _ffi._load_lib().tf_transform_plan_import(
            pointer, length, runtime_pointer,
            ctypes.byref(handle), ctypes.byref(error))
        del owner, runtime
        _check(code, error)
        return _adopt_transferred_handle(
            handle, 'tf_transform_plan_destroy', cls)

    def to_bytes(self, *, limits=None):
        pointer = ctypes.c_void_p()
        length = ctypes.c_size_t()
        error = ctypes.c_void_p()
        lib = _ffi._load_lib()
        code = lib.tf_transform_plan_export(
            self._require_open(), _limits_pointer(limits),
            ctypes.byref(pointer), ctypes.byref(length), ctypes.byref(error))
        _check(code, error)
        try:
            return ctypes.string_at(pointer, length.value)
        finally:
            lib.tf_transform_bytes_free(
                ctypes.byref(pointer), ctypes.byref(length))

    def schema_json(self, which='output', *, limits=None):
        selectors = {
            'input': TF_TRANSFORM_SCHEMA_INPUT,
            'output': TF_TRANSFORM_SCHEMA_OUTPUT,
        }
        if which not in selectors:
            raise ValueError("which must be 'input' or 'output'")
        pointer = ctypes.c_void_p()
        length = ctypes.c_size_t()
        error = ctypes.c_void_p()
        lib = _ffi._load_lib()
        code = lib.tf_transform_plan_schema_json(
            self._require_open(), selectors[which], _limits_pointer(limits),
            ctypes.byref(pointer), ctypes.byref(length), ctypes.byref(error))
        _check(code, error)
        try:
            return ctypes.string_at(pointer, length.value)
        finally:
            lib.tf_transform_bytes_free(
                ctypes.byref(pointer), ctypes.byref(length))

    def apply(self, schema, *, limits=None, cancel_token=None):
        effective_limits = _resolved_limits(limits)
        runtime, runtime_pointer = _runtime(
            effective_limits, cancel_token)
        _raise_if_cancelled(cancel_token)
        schema_owner = _SchemaOwner(
            schema, effective_limits, cancel_token)
        _raise_if_cancelled(cancel_token)
        handle = ctypes.c_void_p()
        error = ctypes.c_void_p()
        code = _ffi._load_lib().tf_transform_apply_create(
            self._require_open(), ctypes.byref(schema_owner.view),
            runtime_pointer, ctypes.byref(handle), ctypes.byref(error))
        del runtime
        _check(code, error)
        return _adopt_transferred_handle(
            handle, 'tf_transform_apply_destroy', TransformApply,
            schema_owner.dtypes, cancel_token, effective_limits)


class TransformApply(_OwnedHandle):
    _destroy_name = 'tf_transform_apply_destroy'

    def __init__(self, handle, dtypes, cancel_token=None, limits=None):
        super().__init__(handle)
        self._dtypes = tuple(dtypes)
        self._cancel_token = cancel_token
        self._limits = limits

    def run(self, table):
        self._require_open()
        _raise_if_cancelled(self._cancel_token, self)
        try:
            owner = _TableOwner(
                table, self._dtypes, self._limits, 'apply',
                self._cancel_token)
        except TranfiTransformError as exc:
            if exc.code == 109:
                self.close()
            raise
        _raise_if_cancelled(self._cancel_token, self)
        dense = _ffi._TfOwnedDenseV1()
        error = ctypes.c_void_p()
        lib = _ffi._load_lib()
        code = lib.tf_transform_apply_run(
            self._require_open(), ctypes.byref(owner.view),
            ctypes.byref(dense), ctypes.byref(error))
        if code == 109:
            self.close()
        _check(code, error)
        try:
            try:
                _raise_if_cancelled(self._cancel_token, self)
                values = array('d')
                offset = 0
                while offset < dense.data_bytes:
                    _raise_if_cancelled(self._cancel_token, self)
                    chunk = min(65_536, dense.data_bytes - offset)
                    values.frombytes(ctypes.string_at(
                        dense.data + offset, chunk))
                    offset += chunk
                _raise_if_cancelled(self._cancel_token, self)
                return DenseResult(
                    rows=int(dense.rows), columns=int(dense.columns), data=values)
            except BaseException:
                self.close()
                raise
        finally:
            lib.tf_owned_dense_free(ctypes.byref(dense))


def safe_transform_limits():
    """Return a fresh mutable-safe limits profile."""
    return TransformLimits()
