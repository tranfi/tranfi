"""
pipeline.py — Pipeline builder and runner.

Usage:
    import tranfi as tf

    p = tf.pipeline([
        tf.codec.csv(delimiter=','),
        tf.ops.filter(tf.expr("col('age') > 25")),
        tf.ops.select(['name', 'age']),
        tf.codec.csv(),
    ])

    result = p.run(input=b'name,age\\nAlice,30\\n')
    print(result.output.decode())
"""

import gzip
import json
import os
from . import _ffi
from .memory_policy import prepare_native_plan, validate_native_memory_policy

# Channel IDs (must match tranfi.h)
CHAN_MAIN = 0
CHAN_ERRORS = 1
CHAN_STATS = 2
CHAN_SAMPLES = 3

CHUNK_SIZE = 64 * 1024  # 64 KB


def _normalize_compression(compression):
    if compression is None:
        return 'auto'
    value = str(compression).lower()
    if value not in ('auto', 'none', 'gzip'):
        raise ValueError("compression must be 'auto', 'none', or 'gzip'")
    return value


def _resolve_file_compression(path, compression):
    mode = _normalize_compression(compression)
    if mode == 'auto':
        return 'gzip' if os.fspath(path).lower().endswith('.gz') else 'none'
    return mode


def _open_input_file(path, compression):
    mode = _resolve_file_compression(path, compression)
    if mode == 'gzip':
        return gzip.open(path, 'rb')
    return open(path, 'rb')


def _has_input(value):
    return value is not None


def _normalize_input_files(input_files):
    if input_files is None:
        return None
    if isinstance(input_files, (str, bytes, os.PathLike)):
        raise TypeError('input_files must be an iterable of paths, not a single path')
    files = [os.fspath(path) for path in input_files]
    if not files:
        raise ValueError('input_files must not be empty')
    return files


def _inject_source_name_step(plan_json, source_column):
    if source_column is None:
        return plan_json
    source_column = str(source_column)
    if not source_column:
        raise ValueError('source_column must be a non-empty column name')
    plan = json.loads(plan_json)
    steps = plan.get('steps')
    if not isinstance(steps, list):
        raise ValueError('plan must contain a steps array')
    insert_at = 0
    if steps:
        first = steps[0]
        first_op = first.get('op') if isinstance(first, dict) else ''
        if isinstance(first_op, str) and first_op.startswith('codec.') and first_op.endswith('.decode'):
            insert_at = 1
    steps.insert(insert_at, {'op': 'source-name', 'args': {'result': source_column}})
    return json.dumps(plan)


class PipelineResult:
    """Result of a pipeline run."""

    def __init__(self, output: bytes, errors: bytes, stats: bytes, samples: bytes):
        self.output = output
        self.errors = errors
        self.stats = stats
        self.samples = samples

    @property
    def output_text(self) -> str:
        return self.output.decode('utf-8')

    @property
    def stats_text(self) -> str:
        return self.stats.decode('utf-8')

    def __repr__(self):
        return (
            f"PipelineResult(output={len(self.output)} bytes, "
            f"errors={len(self.errors)} bytes, "
            f"stats={len(self.stats)} bytes)"
        )


class Pipeline:
    """A configured pipeline ready to run."""

    def __init__(self, steps: list = None, *, recipe: str = None, engine: str = None, dsl: str = None):
        self._engine = engine
        self._dsl = dsl
        if recipe is not None:
            if os.path.isfile(recipe):
                with open(recipe, 'r') as f:
                    self._plan_json = f.read()
            else:
                self._plan_json = recipe
            self._steps = None
        else:
            self._steps = steps or []
            self._plan_json = None

    def _to_plan_json(self) -> str:
        """Convert step list to JSON plan."""
        if self._plan_json is not None:
            return self._plan_json
        return json.dumps({'steps': self._steps})

    def run(self, *, input: bytes = None, input_file: str = None,
            input_files=None, source_column: str = None,
            on_output=None, collect_output: bool = True,
            chunk_size: int = CHUNK_SIZE, allow_blocking: bool = False,
            memory=None, spill_dir: str = None,
            compression: str = 'auto') -> PipelineResult:
        """
        Run the pipeline.

        Args:
            input: Raw bytes to feed into the pipeline.
            input_file: Path to a file to stream through the pipeline.
            input_files: Iterable of file paths streamed sequentially through one pipeline.
            source_column: Optional string column appended with the current input file path.
            compression: `auto` detects `.gz` input files; `gzip` forces gzip; `none` reads raw bytes.
            on_output: Optional callback invoked with each main-output chunk.
            collect_output: If false, drain and discard main output after callbacks.
            chunk_size: Input and output chunk size for native streaming.
            allow_blocking: Permit native full-input blocking steps for known-small data.
            memory: Optional native memory limit, e.g. "64MB" or "max:64MB".
            spill_dir: Native spill directory for supported operators such as sort.

        Returns:
            PipelineResult with output, errors, stats, samples. When
            collect_output is false, result.output is empty.
        """
        compression = _normalize_compression(compression)
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        input_files = _normalize_input_files(input_files)
        source_count = int(_has_input(input)) + int(_has_input(input_file)) + int(_has_input(input_files))
        if source_count > 1:
            raise ValueError('pass only one of input, input_file, or input_files')
        if compression == 'gzip' and input_file is None and input_files is None:
            raise ValueError("compression='gzip' requires input_file or input_files")
        if source_column is not None and input_file is None and input_files is None:
            raise ValueError('source_column requires input_file or input_files')
        if on_output is not None and not callable(on_output):
            raise TypeError("on_output must be callable")
        if self._engine and self._engine != 'native':
            if on_output is not None or not collect_output or input_files is not None or source_column is not None:
                raise RuntimeError("streaming output, input_files, and source_column require the native engine")
            return self._run_engine(input=input, input_file=input_file, memory=memory, spill_dir=spill_dir)

        plan_json = self._to_plan_json()
        plan_json = _inject_source_name_step(plan_json, source_column)
        plan_json = prepare_native_plan(plan_json, allow_blocking=allow_blocking, memory=memory, spill_dir=spill_dir)
        handle = _ffi.pipeline_create(plan_json)
        output_chunks = []
        sink_error = []
        sink_ref = None

        def handle_output(chunk):
            if on_output is not None:
                on_output(chunk)
            if collect_output:
                output_chunks.append(chunk)

        if spill_dir:
            def output_sink(channel, chunk):
                del channel
                try:
                    handle_output(chunk)
                except BaseException as exc:
                    sink_error.append(exc)
                    raise
            sink_ref = _ffi.pipeline_set_sink(handle, CHAN_MAIN, output_sink)

        def raise_sink_error():
            if sink_error:
                raise sink_error[0]

        def drain_main():
            if sink_ref is not None:
                raise_sink_error()
                return
            while True:
                chunk = _ffi.pipeline_pull_chunk(handle, CHAN_MAIN, chunk_size)
                if not chunk:
                    break
                handle_output(chunk)

        def push_file(path):
            path = os.fspath(path)
            _ffi.pipeline_set_source_name(handle, path)
            with _open_input_file(path, compression) as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    _ffi.pipeline_push(handle, chunk)
                    drain_main()
            _ffi.pipeline_flush_input(handle)
            drain_main()

        try:
            if input_files is not None:
                for path in input_files:
                    push_file(path)
            elif input_file:
                push_file(input_file)
            elif input is not None:
                if isinstance(input, str):
                    input = input.encode('utf-8')
                data = bytes(input)
                for offset in range(0, len(data), chunk_size):
                    _ffi.pipeline_push(handle, data[offset:offset + chunk_size])
                    drain_main()

            try:
                _ffi.pipeline_finish(handle)
            except RuntimeError:
                raise_sink_error()
                raise
            drain_main()
            raise_sink_error()

            output = b''.join(output_chunks) if collect_output else b''
            errors = _ffi.pipeline_pull(handle, CHAN_ERRORS)
            stats = _ffi.pipeline_pull(handle, CHAN_STATS)
            samples = _ffi.pipeline_pull(handle, CHAN_SAMPLES)

            return PipelineResult(output, errors, stats, samples)
        finally:
            del sink_ref
            _ffi.pipeline_free(handle)

    def iter_chunks(self, *, input: bytes = None, input_file: str = None,
                    input_files=None, source_column: str = None,
                    chunk_size: int = CHUNK_SIZE, allow_blocking: bool = False,
                    memory=None, spill_dir: str = None,
                    compression: str = 'auto'):
        """Yield main-output chunks while the native pipeline runs."""
        compression = _normalize_compression(compression)
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        input_files = _normalize_input_files(input_files)
        source_count = int(_has_input(input)) + int(_has_input(input_file)) + int(_has_input(input_files))
        if source_count > 1:
            raise ValueError('pass only one of input, input_file, or input_files')
        if compression == 'gzip' and input_file is None and input_files is None:
            raise ValueError("compression='gzip' requires input_file or input_files")
        if source_column is not None and input_file is None and input_files is None:
            raise ValueError('source_column requires input_file or input_files')
        if self._engine and self._engine != 'native':
            raise RuntimeError("streaming chunk iteration requires the native engine")

        plan_json = self._to_plan_json()
        plan_json = _inject_source_name_step(plan_json, source_column)
        plan_json = prepare_native_plan(plan_json, allow_blocking=allow_blocking, memory=memory, spill_dir=spill_dir)
        handle = _ffi.pipeline_create(plan_json)

        def drain_main():
            while True:
                chunk = _ffi.pipeline_pull_chunk(handle, CHAN_MAIN, chunk_size)
                if not chunk:
                    break
                yield chunk

        def push_file_chunks(path):
            path = os.fspath(path)
            _ffi.pipeline_set_source_name(handle, path)
            with _open_input_file(path, compression) as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    _ffi.pipeline_push(handle, chunk)
                    yield from drain_main()
            _ffi.pipeline_flush_input(handle)
            yield from drain_main()

        try:
            if input_files is not None:
                for path in input_files:
                    yield from push_file_chunks(path)
            elif input_file:
                yield from push_file_chunks(input_file)
            elif input is not None:
                if isinstance(input, str):
                    input = input.encode('utf-8')
                data = bytes(input)
                for offset in range(0, len(data), chunk_size):
                    _ffi.pipeline_push(handle, data[offset:offset + chunk_size])
                    yield from drain_main()

            while True:
                done = _ffi.pipeline_finish_step(handle)
                yield from drain_main()
                if done:
                    break
        finally:
            _ffi.pipeline_free(handle)

    def _run_engine(self, *, input: bytes = None, input_file: str = None,
                    input_files=None, source_column: str = None,
                    memory=None, spill_dir: str = None) -> PipelineResult:
        """Run pipeline via an alternative engine (e.g. duckdb)."""
        if input_files is not None or source_column is not None:
            raise RuntimeError('input_files and source_column require the native engine')
        from .engines import get_engine
        engine = get_engine(self._engine)
        return engine.run(self._dsl, input=input, input_file=input_file,
                          memory=memory, spill_dir=spill_dir)


def pipeline(steps=None, *, recipe: str = None, engine: str = None) -> Pipeline:
    """Create a pipeline from step list, DSL string, recipe name, or recipe file.

    Examples:
        tf.pipeline([tf.codec.csv(), tf.ops.head(10), tf.codec.csv()])
        tf.pipeline('csv | head 10 | csv')
        tf.pipeline('preview')  # built-in recipe
        tf.pipeline(recipe='/path/to/recipe.tranfi')
        tf.pipeline('csv | filter "age > 25" | csv', engine='duckdb')
    """
    if recipe is not None:
        return Pipeline(recipe=recipe, engine=engine)
    if isinstance(steps, str):
        # Check if it's a built-in recipe name (no pipes, no braces)
        s = steps.strip()
        if '|' not in s and not s.startswith('{'):
            found_dsl = _ffi.recipe_find_dsl(s)
            if found_dsl:
                return Pipeline(recipe=_ffi.compile_dsl(found_dsl), engine=engine, dsl=found_dsl)
        # DSL string or JSON
        if not s.startswith('{'):
            return Pipeline(recipe=_ffi.compile_dsl(s), engine=engine, dsl=s)
        return Pipeline(recipe=s, engine=engine)
    return Pipeline(steps, recipe=recipe, engine=engine)


def param(name: str, default=None):
    """Create a parameter reference (for future use with parameterized pipelines)."""
    result = {'param': name}
    if default is not None:
        result['default'] = default
    return result


def expr(text: str) -> str:
    """Mark a string as an expression (currently just returns the string)."""
    return text


def load_recipe(source: str) -> Pipeline:
    """Load a recipe from a .tranfi file path or JSON string.

    Args:
        source: Path to a .tranfi file, or a JSON string.

    Returns:
        A Pipeline ready to run.
    """
    return Pipeline(recipe=source)


def save_recipe(steps: list, path: str) -> None:
    """Save pipeline steps to a .tranfi recipe file.

    Args:
        steps: List of step dicts (e.g. from codec/ops builders).
        path: Output file path.
    """
    with open(path, 'w') as f:
        json.dump({'steps': steps}, f)


def compile_dsl(dsl: str) -> str:
    """Compile a DSL string to a JSON recipe string.

    Args:
        dsl: Pipe DSL string, e.g. "csv | filter \\"col('age') > 25\\" | csv"

    Returns:
        JSON string suitable for saving as a .tranfi file.
    """
    return _ffi.compile_dsl(dsl)
