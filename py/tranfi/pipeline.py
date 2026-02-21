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

import json
import os
from . import _ffi

# Channel IDs (must match tranfi.h)
CHAN_MAIN = 0
CHAN_ERRORS = 1
CHAN_STATS = 2
CHAN_SAMPLES = 3

CHUNK_SIZE = 64 * 1024  # 64 KB


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

    def run(self, *, input: bytes = None, input_file: str = None) -> PipelineResult:
        """
        Run the pipeline.

        Args:
            input: Raw bytes to feed into the pipeline.
            input_file: Path to a file to stream through the pipeline.

        Returns:
            PipelineResult with output, errors, stats, samples.
        """
        if self._engine and self._engine != 'native':
            return self._run_engine(input=input, input_file=input_file)

        plan_json = self._to_plan_json()
        handle = _ffi.pipeline_create(plan_json)

        try:
            if input_file:
                with open(input_file, 'rb') as f:
                    while True:
                        chunk = f.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        _ffi.pipeline_push(handle, chunk)
            elif input is not None:
                _ffi.pipeline_push(handle, input)

            _ffi.pipeline_finish(handle)

            output = _ffi.pipeline_pull(handle, CHAN_MAIN)
            errors = _ffi.pipeline_pull(handle, CHAN_ERRORS)
            stats = _ffi.pipeline_pull(handle, CHAN_STATS)
            samples = _ffi.pipeline_pull(handle, CHAN_SAMPLES)

            return PipelineResult(output, errors, stats, samples)
        finally:
            _ffi.pipeline_free(handle)

    def _run_engine(self, *, input: bytes = None, input_file: str = None) -> PipelineResult:
        """Run pipeline via an alternative engine (e.g. duckdb)."""
        from .engines import get_engine
        engine = get_engine(self._engine)
        return engine.run(self._dsl, input=input, input_file=input_file)


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
