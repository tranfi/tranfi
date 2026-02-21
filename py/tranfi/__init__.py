"""
tranfi — Streaming ETL language + runtime.

Usage:
    import tranfi as tf

    p = tf.pipeline([
        tf.codec.csv(delimiter=','),
        tf.ops.filter(tf.expr("col('age') > 25")),
        tf.ops.select(['name', 'age']),
        tf.codec.csv(),
    ])

    result = p.run(input_file='data.csv')
    print(result.output_text)
"""

from .pipeline import (pipeline, param, expr, Pipeline, PipelineResult,
                       load_recipe, save_recipe, compile_dsl)
from ._ffi import version
from . import _ffi

__version__ = '0.1.0'


def recipes():
    """List built-in recipes.

    Returns list of {'name': str, 'dsl': str, 'description': str} dicts.
    """
    n = _ffi.recipe_count()
    return [
        {
            'name': _ffi.recipe_name(i),
            'dsl': _ffi.recipe_dsl(i),
            'description': _ffi.recipe_description(i),
        }
        for i in range(n)
    ]


class codec:
    """Codec step constructors."""

    @staticmethod
    def csv(delimiter=',', header=True, batch_size=1024, encode=False):
        """CSV codec. Use encode=True for encoding (output), False for decoding (input)."""
        args = {}
        if delimiter != ',':
            args['delimiter'] = delimiter
        if not header:
            args['header'] = False
        if batch_size != 1024:
            args['batch_size'] = batch_size
        op = 'codec.csv.encode' if encode else 'codec.csv.decode'
        return {'op': op, 'args': args}

    @staticmethod
    def csv_decode(delimiter=',', header=True, batch_size=1024):
        """CSV decoder."""
        return codec.csv(delimiter=delimiter, header=header, batch_size=batch_size, encode=False)

    @staticmethod
    def csv_encode(delimiter=','):
        """CSV encoder."""
        args = {}
        if delimiter != ',':
            args['delimiter'] = delimiter
        return {'op': 'codec.csv.encode', 'args': args}

    @staticmethod
    def jsonl(batch_size=1024, encode=False):
        """JSON Lines codec."""
        args = {}
        if batch_size != 1024:
            args['batch_size'] = batch_size
        op = 'codec.jsonl.encode' if encode else 'codec.jsonl.decode'
        return {'op': op, 'args': args}

    @staticmethod
    def jsonl_decode(batch_size=1024):
        """JSON Lines decoder."""
        return codec.jsonl(batch_size=batch_size, encode=False)

    @staticmethod
    def jsonl_encode():
        """JSON Lines encoder."""
        return {'op': 'codec.jsonl.encode', 'args': {}}

    @staticmethod
    def text(batch_size=1024, encode=False):
        """Text line codec."""
        args = {}
        if batch_size != 1024:
            args['batch_size'] = batch_size
        op = 'codec.text.encode' if encode else 'codec.text.decode'
        return {'op': op, 'args': args}

    @staticmethod
    def text_decode(batch_size=1024):
        """Text line decoder."""
        return codec.text(batch_size=batch_size, encode=False)

    @staticmethod
    def text_encode():
        """Text line encoder."""
        return {'op': 'codec.text.encode', 'args': {}}


class ops:
    """Transform step constructors."""

    @staticmethod
    def filter(expression):
        """Filter rows by expression. Example: tf.ops.filter(tf.expr("col('x') > 0"))"""
        return {'op': 'filter', 'args': {'expr': expression}}

    @staticmethod
    def select(columns):
        """Select and reorder columns. Example: tf.ops.select(['name', 'age'])"""
        return {'op': 'select', 'args': {'columns': columns}}

    @staticmethod
    def rename(**mapping):
        """Rename columns. Example: tf.ops.rename(name='full_name', age='years')"""
        return {'op': 'rename', 'args': {'mapping': mapping}}

    @staticmethod
    def head(n):
        """Take first N rows. Example: tf.ops.head(10)"""
        return {'op': 'head', 'args': {'n': n}}

    @staticmethod
    def skip(n):
        """Skip first N rows. Example: tf.ops.skip(5)"""
        return {'op': 'skip', 'args': {'n': n}}

    @staticmethod
    def derive(columns):
        """Create derived columns. Example: tf.ops.derive({'total': expr("col('a') + col('b')")})"""
        if isinstance(columns, dict):
            cols = [{'name': k, 'expr': v} for k, v in columns.items()]
        else:
            cols = columns
        return {'op': 'derive', 'args': {'columns': cols}}

    @staticmethod
    def stats(stats_list=None):
        """Compute column statistics. Example: tf.ops.stats(['count', 'mean', 'min', 'max'])"""
        args = {}
        if stats_list is not None:
            args['stats'] = stats_list
        return {'op': 'stats', 'args': args}

    @staticmethod
    def unique(columns=None):
        """Keep unique rows. Example: tf.ops.unique(['name', 'city'])"""
        args = {}
        if columns is not None:
            args['columns'] = columns
        return {'op': 'unique', 'args': args}

    @staticmethod
    def sort(columns):
        """Sort rows. Example: tf.ops.sort(['age', '-name'])"""
        cols = []
        for c in columns:
            if isinstance(c, str):
                desc = c.startswith('-')
                cols.append({'name': c[1:] if desc else c, 'desc': desc})
            else:
                cols.append(c)
        return {'op': 'sort', 'args': {'columns': cols}}

    @staticmethod
    def tail(n):
        """Take last N rows. Example: tf.ops.tail(10)"""
        return {'op': 'tail', 'args': {'n': n}}

    @staticmethod
    def validate(expression):
        """Add _valid bool column. Example: tf.ops.validate(expr("col('age') > 0"))"""
        return {'op': 'validate', 'args': {'expr': expression}}

    @staticmethod
    def trim(columns=None):
        """Trim whitespace from string columns. Example: tf.ops.trim(['name', 'city'])"""
        args = {}
        if columns is not None:
            args['columns'] = columns
        return {'op': 'trim', 'args': args}

    @staticmethod
    def fill_null(**mapping):
        """Replace nulls with defaults. Example: tf.ops.fill_null(age='0', city='unknown')"""
        return {'op': 'fill-null', 'args': {'mapping': mapping}}

    @staticmethod
    def cast(**mapping):
        """Type conversion. Example: tf.ops.cast(age='int', score='float')"""
        return {'op': 'cast', 'args': {'mapping': mapping}}

    @staticmethod
    def clip(column, min=None, max=None):
        """Clamp numeric values. Example: tf.ops.clip('score', min=0, max=100)"""
        args = {'column': column}
        if min is not None:
            args['min'] = min
        if max is not None:
            args['max'] = max
        return {'op': 'clip', 'args': args}

    @staticmethod
    def replace(column, pattern, replacement, regex=False):
        """String find/replace. Example: tf.ops.replace('name', 'foo', 'bar', regex=True)"""
        args = {'column': column, 'pattern': pattern, 'replacement': replacement}
        if regex:
            args['regex'] = True
        return {'op': 'replace', 'args': args}

    @staticmethod
    def hash(columns=None):
        """DJB2 hash of columns, adds _hash column. Example: tf.ops.hash(['name', 'city'])"""
        args = {}
        if columns is not None:
            args['columns'] = columns
        return {'op': 'hash', 'args': args}

    @staticmethod
    def bin(column, boundaries):
        """Discretize into bins. Example: tf.ops.bin('age', [18, 30, 50])"""
        return {'op': 'bin', 'args': {'column': column, 'boundaries': boundaries}}

    @staticmethod
    def fill_down(columns=None):
        """Forward-fill nulls. Example: tf.ops.fill_down(['city'])"""
        args = {}
        if columns is not None:
            args['columns'] = columns
        return {'op': 'fill-down', 'args': args}

    @staticmethod
    def step(column, func, result=None):
        """Running aggregation. Example: tf.ops.step('price', 'running-sum', 'cumsum')"""
        args = {'column': column, 'func': func}
        if result is not None:
            args['result'] = result
        return {'op': 'step', 'args': args}

    @staticmethod
    def window(column, size, func, result=None):
        """Sliding window aggregation. Example: tf.ops.window('price', 3, 'avg', 'price_ma3')"""
        args = {'column': column, 'size': size, 'func': func}
        if result is not None:
            args['result'] = result
        return {'op': 'window', 'args': args}

    @staticmethod
    def explode(column, delimiter=','):
        """Split delimited string into multiple rows. Example: tf.ops.explode('tags', ',')"""
        args = {'column': column}
        if delimiter != ',':
            args['delimiter'] = delimiter
        return {'op': 'explode', 'args': args}

    @staticmethod
    def split(column, names, delimiter=' '):
        """Split column into multiple columns. Example: tf.ops.split('name', ['first', 'last'])"""
        args = {'column': column, 'names': names}
        if delimiter != ' ':
            args['delimiter'] = delimiter
        return {'op': 'split', 'args': args}

    @staticmethod
    def unpivot(columns):
        """Wide to long. Example: tf.ops.unpivot(['jan', 'feb', 'mar'])"""
        return {'op': 'unpivot', 'args': {'columns': columns}}

    @staticmethod
    def top(n, column, desc=True):
        """Top N by column. Example: tf.ops.top(10, 'score')"""
        return {'op': 'top', 'args': {'n': n, 'column': column, 'desc': desc}}

    @staticmethod
    def sample(n):
        """Reservoir sampling. Example: tf.ops.sample(100)"""
        return {'op': 'sample', 'args': {'n': n}}

    @staticmethod
    def group_agg(group_by, aggs):
        """Group by + aggregate. Example: tf.ops.group_agg(['city'], [{'column': 'price', 'func': 'sum', 'result': 'total'}])"""
        return {'op': 'group-agg', 'args': {'group_by': group_by, 'aggs': aggs}}

    @staticmethod
    def frequency(columns=None):
        """Value counts. Example: tf.ops.frequency(['city'])"""
        args = {}
        if columns is not None:
            args['columns'] = columns
        return {'op': 'frequency', 'args': args}

    @staticmethod
    def datetime(column, extract=None):
        """Extract date components. Example: tf.ops.datetime('date', ['year', 'month', 'day'])"""
        args = {'column': column}
        if extract is not None:
            args['extract'] = extract
        return {'op': 'datetime', 'args': args}

    @staticmethod
    def reorder(columns):
        """Reorder columns. Alias for select. Example: tf.ops.reorder(['name', 'age'])"""
        return {'op': 'reorder', 'args': {'columns': columns}}

    @staticmethod
    def dedup(columns=None):
        """Deduplicate rows. Alias for unique. Example: tf.ops.dedup(['name'])"""
        args = {}
        if columns is not None:
            args['columns'] = columns
        return {'op': 'dedup', 'args': args}

    @staticmethod
    def grep(pattern, invert=False, column='_line', regex=False):
        """Substring/regex filter. Example: tf.ops.grep('^err', regex=True)"""
        args = {'pattern': pattern}
        if invert:
            args['invert'] = True
        if column != '_line':
            args['column'] = column
        if regex:
            args['regex'] = True
        return {'op': 'grep', 'args': args}

    @staticmethod
    def flatten():
        """Flatten nested columns (passthrough for already-flat data)."""
        return {'op': 'flatten', 'args': {}}


class io:
    """I/O step constructors (for future use with connectors)."""

    class read:
        @staticmethod
        def file(path=None):
            return {'op': 'io.read.file', 'args': {'path': path}}

    class write:
        @staticmethod
        def stdout():
            return {'op': 'io.write.stdout', 'args': {}}
