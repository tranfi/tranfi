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
from ._ffi import compile_to_sql
from ._ffi import version
from . import _ffi

__version__ = '0.1.2'


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
    def csv(delimiter=',', header=True, batch_size=1024, encode=False, repair=False,
            nulls=None, quoted_nulls=True, mode=None, strict=False, max_error_bytes=4096,
            max_record_bytes=64 * 1024 * 1024, comment=None, trim_ws=True,
            skip_empty_rows=False, skip=0, n_max=None, max_rows=None, audit=False, audit_limit=None):
        """CSV codec. Use encode=True for encoding (output), False for decoding (input)."""
        args = {}
        if delimiter != ',':
            args['delimiter'] = delimiter
        if not header:
            args['header'] = False
        if batch_size != 1024:
            args['batch_size'] = batch_size
        if repair:
            args['repair'] = True
        if not encode:
            if mode is not None:
                args['mode'] = mode
            if strict:
                args['strict'] = True
            if max_error_bytes != 4096:
                args['max_error_bytes'] = int(max_error_bytes)
            if max_record_bytes != 64 * 1024 * 1024:
                args['max_record_bytes'] = int(max_record_bytes)
            if audit:
                args['audit'] = True
            if audit_limit is not None:
                args['audit_limit'] = int(audit_limit)
            if nulls is not None:
                args['nulls'] = nulls
            if quoted_nulls is not True:
                args['quoted_nulls'] = bool(quoted_nulls)
            if comment:
                args['comment'] = str(comment)
            if trim_ws is not True:
                args['trim_ws'] = bool(trim_ws)
            if skip_empty_rows:
                args['skip_empty_rows'] = True
            if skip:
                args['skip'] = int(skip)
            if n_max is not None:
                args['n_max'] = int(n_max)
            elif max_rows is not None:
                args['max_rows'] = int(max_rows)
        op = 'codec.csv.encode' if encode else 'codec.csv.decode'
        return {'op': op, 'args': args}

    @staticmethod
    def csv_decode(delimiter=',', header=True, batch_size=1024, nulls=None, quoted_nulls=True,
                   mode=None, strict=False, max_error_bytes=4096,
                   max_record_bytes=64 * 1024 * 1024, comment=None, trim_ws=True,
                   skip_empty_rows=False, skip=0, n_max=None, max_rows=None, audit=False, audit_limit=None):
        """CSV decoder."""
        return codec.csv(delimiter=delimiter, header=header, batch_size=batch_size,
                         encode=False, nulls=nulls, quoted_nulls=quoted_nulls,
                         mode=mode, strict=strict, max_error_bytes=max_error_bytes,
                         max_record_bytes=max_record_bytes, comment=comment,
                         trim_ws=trim_ws, skip_empty_rows=skip_empty_rows, skip=skip,
                         n_max=n_max, max_rows=max_rows, audit=audit, audit_limit=audit_limit)

    @staticmethod
    def csv_encode(delimiter=','):
        """CSV encoder."""
        args = {}
        if delimiter != ',':
            args['delimiter'] = delimiter
        return {'op': 'codec.csv.encode', 'args': args}

    @staticmethod
    def jsonl(batch_size=1024, encode=False, on_error='skip', max_error_bytes=4096):
        """JSON Lines codec."""
        args = {}
        if batch_size != 1024:
            args['batch_size'] = batch_size
        op = 'codec.jsonl.encode' if encode else 'codec.jsonl.decode'
        if not encode:
            if on_error != 'skip':
                args['on_error'] = on_error
            if max_error_bytes != 4096:
                args['max_error_bytes'] = max_error_bytes
        return {'op': op, 'args': args}

    @staticmethod
    def jsonl_decode(batch_size=1024, on_error='skip', max_error_bytes=4096):
        """JSON Lines decoder."""
        return codec.jsonl(batch_size=batch_size, encode=False,
                           on_error=on_error, max_error_bytes=max_error_bytes)

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

    @staticmethod
    def table_encode(max_width=40, max_rows=0):
        """Pretty-print Markdown table encoder."""
        args = {}
        if max_width != 40:
            args['max_width'] = max_width
        if max_rows != 0:
            args['max_rows'] = max_rows
        return {'op': 'codec.table.encode', 'args': args}


class ops:
    """Transform step constructors."""

    @staticmethod
    def filter(expression, audit=False, audit_limit=None):
        """Filter rows by expression. Set audit=True to record bounded dropped-row audit entries."""
        args = {'expr': expression}
        if audit:
            args['audit'] = True
        if audit_limit is not None:
            args['audit_limit'] = audit_limit
        return {'op': 'filter', 'args': args}

    @staticmethod
    def select(columns):
        """Select and reorder columns. Example: tf.ops.select(['name', 'age'])"""
        return {'op': 'select', 'args': {'columns': columns}}

    @staticmethod
    def relocate(columns, before=None, after=None):
        """Move columns while keeping all columns. Example: tf.ops.relocate(['city'], before='age')"""
        args = {'columns': columns}
        if before is not None:
            args['before'] = before
        if after is not None:
            args['after'] = after
        return {'op': 'relocate', 'args': args}

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
    def source_name(result='_source', default=''):
        """Append the current host source name as a string column."""
        args = {}
        if result != '_source':
            args['result'] = result
        if default != '':
            args['default'] = default
        return {'op': 'source-name', 'args': args}


    @staticmethod
    def across(columns, functions=None, fn=None, names=None, replace=None):
        """Apply row-local function(s) across selected columns."""
        args = {'columns': columns}
        if functions is not None:
            args['functions'] = functions if isinstance(functions, (list, tuple)) else [functions]
        elif fn is not None:
            args['fn'] = fn
        else:
            raise ValueError('across requires functions or fn')
        if names is not None:
            args['names'] = names
        if replace is not None:
            args['replace'] = bool(replace)
        return {'op': 'across', 'args': args}

    @staticmethod
    def stats(stats_list=None):
        """Compute column statistics. Example: tf.ops.stats(['count', 'avg', 'min', 'max'])"""
        args = {}
        if stats_list is not None:
            args['stats'] = stats_list
        return {'op': 'stats', 'args': args}

    @staticmethod
    def scan(stats_list=None):
        """Stream a bounded per-column profile. Example: tf.ops.scan()"""
        args = {}
        if stats_list is not None:
            args['stats'] = stats_list
        return {'op': 'scan', 'args': args}

    @staticmethod
    def unique(columns=None, max_keys=None, sorted=False, max_state_bytes=None):
        """Keep unique rows. Use sorted=True for adjacent-key streaming mode."""
        args = {}
        if columns is not None:
            args['columns'] = columns
        if max_keys is not None:
            args['max_keys'] = max_keys
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if sorted:
            args['sorted'] = True
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
    def validate(expression=None, audit=False, audit_limit=None, max_failures=None,
                 max_failure_rate=None, warn_failure_rate=None, name=None,
                 message=None, rules=None, rules_file=None):
        """Add _valid bool column from one expression or a row-local rule set."""
        args = {}
        if rules_file is not None:
            args['rules_file'] = str(rules_file)
        if rules is not None:
            args['rules'] = rules
        if expression is not None:
            args['expr'] = expression
        if not any(k in args for k in ('expr', 'rules', 'rules_file')):
            raise ValueError('validate requires expression, rules, or rules_file')
        if audit:
            args['audit'] = True
        if audit_limit is not None:
            args['audit_limit'] = int(audit_limit)
        if max_failures is not None:
            args['max_failures'] = int(max_failures)
        if max_failure_rate is not None:
            args['max_failure_rate'] = float(max_failure_rate)
        if warn_failure_rate is not None:
            args['warn_failure_rate'] = float(warn_failure_rate)
        if name is not None:
            args['name'] = name
        if message:
            args['message'] = message
        return {'op': 'validate', 'args': args}

    @staticmethod
    def assert_(expression=None, action='fail', name='assert', message='', result='_assert', audit=False,
                audit_limit=None, aggregate=None, op=None, value=None, column=None):
        """Assert a row-local rule or a finish-time streaming aggregate rule."""
        args = {'action': action}
        if aggregate is not None:
            if expression is not None:
                raise ValueError('assert_ accepts either expression or aggregate, not both')
            if op is None or value is None:
                raise ValueError('aggregate assert requires op and value')
            args['aggregate'] = aggregate
            args['op'] = op
            args['value'] = float(value)
            if column is not None:
                args['column'] = column
        elif expression is not None:
            args['expr'] = expression
        else:
            raise ValueError('assert_ requires expression or aggregate')
        if name is not None:
            args['name'] = name
        if message:
            args['message'] = message
        if result is not None:
            args['result'] = result
        if audit:
            args['audit'] = True
        if audit_limit is not None:
            args['audit_limit'] = audit_limit
        return {'op': 'assert', 'args': args}

    @staticmethod
    def quarantine(expression, name=None, message=None):
        """Route rows matching expression to errors and drop them from main output."""
        args = {'expr': expression}
        if name is not None:
            args['name'] = name
        if message:
            args['message'] = message
        return {'op': 'quarantine', 'args': args}

    @staticmethod
    def schema(columns=None, required=None, non_null=None, nullable=None, values=None,
               min=None, max=None, regex=None, mode='fail', name='schema',
               message='', result='_schema', audit=False, audit_limit=None):
        """Validate a row-local column contract. mode: fail, warn, filter, quarantine, annotate."""
        args = {'mode': mode, 'action': mode}
        if columns is not None:
            args['columns'] = columns
        if required is not None:
            args['required'] = required
        if non_null is not None:
            args['non_null'] = non_null
        if nullable is not None:
            args['nullable'] = nullable
        if values is not None:
            args['values'] = values
        if min is not None:
            args['min'] = min
        if max is not None:
            args['max'] = max
        if regex is not None:
            args['regex'] = regex
        if name is not None:
            args['name'] = name
        if message:
            args['message'] = message
        if result is not None:
            args['result'] = result
        if audit:
            args['audit'] = True
        if audit_limit is not None:
            args['audit_limit'] = audit_limit
        return {'op': 'schema', 'args': args}

    @staticmethod
    def schema_infer(rows=None):
        """Infer a bounded schema report. rows limits sampled rows, default 10000."""
        args = {}
        if rows is not None:
            args['rows'] = rows
        return {'op': 'schema-infer', 'args': args}

    @staticmethod
    def tee(expr=None, channel='samples', columns=None, limit=1000, every=1, name='tee', include_row=True):
        """Copy bounded row snapshots to a side channel while preserving main rows."""
        args = {
            'channel': channel,
            'limit': limit,
            'every': every,
            'name': name,
            'include_row': include_row,
        }
        if expr is not None:
            args['expr'] = expr
        if columns is not None:
            args['columns'] = columns
        return {'op': 'tee', 'args': args}

    @staticmethod
    def trim(columns=None):
        """Trim whitespace from string columns. Example: tf.ops.trim(['name', 'city'])"""
        args = {}
        if columns is not None:
            args['columns'] = columns
        return {'op': 'trim', 'args': args}

    @staticmethod
    def fill_null(audit=False, audit_limit=None, **mapping):
        """Replace nulls with defaults. Example: tf.ops.fill_null(age='0', city='unknown')"""
        args = {'mapping': mapping}
        if audit:
            args['audit'] = True
        if audit_limit is not None:
            args['audit_limit'] = int(audit_limit)
        return {'op': 'fill-null', 'args': args}

    @staticmethod
    def cast(audit=False, audit_limit=None, **mapping):
        """Type conversion. Example: tf.ops.cast(age='int', score='float')"""
        args = {'mapping': mapping}
        if audit:
            args['audit'] = True
        if audit_limit is not None:
            args['audit_limit'] = int(audit_limit)
        return {'op': 'cast', 'args': args}

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
    def replace(column, pattern, replacement, regex=False, audit=False, audit_limit=None):
        """String find/replace. Example: tf.ops.replace('name', 'foo', 'bar', regex=True)"""
        args = {'column': column, 'pattern': pattern, 'replacement': replacement}
        if regex:
            args['regex'] = True
        if audit:
            args['audit'] = True
        if audit_limit is not None:
            args['audit_limit'] = int(audit_limit)
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
    def rolling_sum(column, size, result=None):
        """Trailing fixed-window sum. Example: tf.ops.rolling_sum('price', 3, 'price_sum3')"""
        args = {'column': column, 'size': size}
        if result is not None:
            args['result'] = result
        return {'op': 'rolling-sum', 'args': args}

    @staticmethod
    def rolling_mean(column, size, result=None):
        """Trailing fixed-window mean. Example: tf.ops.rolling_mean('price', 3, 'price_ma3')"""
        args = {'column': column, 'size': size}
        if result is not None:
            args['result'] = result
        return {'op': 'rolling-mean', 'args': args}

    @staticmethod
    def rolling_min(column, size, result=None):
        """Trailing fixed-window minimum. Example: tf.ops.rolling_min('price', 3, 'price_min3')"""
        args = {'column': column, 'size': size}
        if result is not None:
            args['result'] = result
        return {'op': 'rolling-min', 'args': args}

    @staticmethod
    def rolling_max(column, size, result=None):
        """Trailing fixed-window maximum. Example: tf.ops.rolling_max('price', 3, 'price_max3')"""
        args = {'column': column, 'size': size}
        if result is not None:
            args['result'] = result
        return {'op': 'rolling-max', 'args': args}

    @staticmethod
    def rolling_any(column, size, result=None, nulls='ignore'):
        """Trailing fixed-window logical any. nulls: ignore, false, true, or propagate."""
        args = {'column': column, 'size': size}
        if result is not None:
            args['result'] = result
        if nulls != 'ignore':
            args['nulls'] = nulls
        return {'op': 'rolling-any', 'args': args}

    @staticmethod
    def rolling_all(column, size, result=None, nulls='ignore'):
        """Trailing fixed-window logical all. nulls: ignore, false, true, or propagate."""
        args = {'column': column, 'size': size}
        if result is not None:
            args['result'] = result
        if nulls != 'ignore':
            args['nulls'] = nulls
        return {'op': 'rolling-all', 'args': args}

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
    def pivot(name_column, value_column, agg=None, categories=None, max_categories=None, sorted=False):
        """Long to wide pivot. Use categories + sorted=True for bounded grouped-input execution."""
        args = {'name_column': name_column, 'value_column': value_column}
        if agg is not None:
            args['agg'] = agg
        if categories is not None:
            args['categories'] = categories
        if max_categories is not None:
            args['max_categories'] = max_categories
        if sorted:
            args['sorted'] = True
        return {'op': 'pivot', 'args': args}

    @staticmethod
    def normalize(columns, method='minmax', audit=False, audit_limit=None):
        """Normalize numeric columns with minmax or zscore. Example: tf.ops.normalize(['score'])"""
        if isinstance(columns, str):
            cols = [c.strip() for c in columns.split(',') if c.strip()]
        else:
            cols = columns
        args = {'columns': cols, 'method': method}
        if audit:
            args['audit'] = True
        if audit_limit is not None:
            args['audit_limit'] = int(audit_limit)
        return {'op': 'normalize', 'args': args}

    @staticmethod
    def top(n, column, desc=True):
        """Top N by column. Example: tf.ops.top(10, 'score')"""
        return {'op': 'top', 'args': {'n': n, 'column': column, 'desc': desc}}

    @staticmethod
    def top_k(n, column):
        """Bounded top-k rows by column. Example: tf.ops.top_k(10, 'score')"""
        return {'op': 'top-k', 'args': {'n': n, 'column': column, 'desc': True}}

    @staticmethod
    def bottom_k(n, column):
        """Bounded bottom-k rows by column. Example: tf.ops.bottom_k(10, 'score')"""
        return {'op': 'bottom-k', 'args': {'n': n, 'column': column, 'desc': False}}

    @staticmethod
    def slice_head(n):
        """Take first N rows using dplyr slice vocabulary. Example: tf.ops.slice_head(10)"""
        return {'op': 'slice-head', 'args': {'n': n}}

    @staticmethod
    def slice_tail(n):
        """Take last N rows using dplyr slice vocabulary. Example: tf.ops.slice_tail(10)"""
        return {'op': 'slice-tail', 'args': {'n': n}}

    @staticmethod
    def slice_min(column, n=1, with_ties=False):
        """Bounded smallest rows by column. Example: tf.ops.slice_min('score', n=5, with_ties=False)"""
        if with_ties:
            raise ValueError('slice_min with_ties=True is not supported by bounded exact-N execution')
        return {'op': 'slice-min', 'args': {'n': n, 'column': column, 'desc': False, 'with_ties': False}}

    @staticmethod
    def slice_max(column, n=1, with_ties=False):
        """Bounded largest rows by column. Example: tf.ops.slice_max('score', n=5, with_ties=False)"""
        if with_ties:
            raise ValueError('slice_max with_ties=True is not supported by bounded exact-N execution')
        return {'op': 'slice-max', 'args': {'n': n, 'column': column, 'desc': True, 'with_ties': False}}

    @staticmethod
    def sample(n):
        """Reservoir sampling. Example: tf.ops.sample(100)"""
        return {'op': 'sample', 'args': {'n': n}}

    @staticmethod
    def group_agg(group_by, aggs, max_groups=None, sorted=False, max_state_bytes=None):
        """Group by + aggregate. Use sorted=True for consecutive-group streaming mode."""
        args = {'group_by': group_by, 'aggs': aggs}
        if max_groups is not None:
            args['max_groups'] = max_groups
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if sorted:
            args['sorted'] = True
        return {'op': 'group-agg', 'args': args}

    @staticmethod
    def frequency(columns=None, max_values=None, overflow=None, other=None, max_state_bytes=None,
                  audit=False, audit_limit=None):
        """Value counts. Example: tf.ops.frequency(['city'], max_values=10000, overflow='other')"""
        args = {}
        if columns is not None:
            args['columns'] = columns
        if max_values is not None:
            args['max_values'] = max_values
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if overflow is not None:
            args['overflow'] = overflow
        if other is not None:
            args['other'] = other
        if audit:
            args['audit'] = True
        if audit_limit is not None:
            args['audit_limit'] = int(audit_limit)
        return {'op': 'frequency', 'args': args}

    @staticmethod
    def onehot(column, drop=False, categories=None, max_categories=None, unknown=None, max_state_bytes=None):
        """One-hot encode a category column. Example: tf.ops.onehot('city', categories=['NY', 'LA'], unknown='other')"""
        args = {'column': column}
        if drop:
            args['drop'] = True
        if categories is not None:
            args['categories'] = categories
        if max_categories is not None:
            args['max_categories'] = max_categories
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if unknown is not None:
            args['unknown'] = unknown
        return {'op': 'onehot', 'args': args}

    @staticmethod
    def label_encode(column, result=None, categories=None, max_categories=None, unknown=None, max_state_bytes=None):
        """Map categories to integer labels. Example: tf.ops.label_encode('city', categories=['NY', 'LA'], unknown='null')"""
        args = {'column': column}
        if result is not None:
            args['result'] = result
        if categories is not None:
            args['categories'] = categories
        if max_categories is not None:
            args['max_categories'] = max_categories
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if unknown is not None:
            args['unknown'] = unknown
        return {'op': 'label-encode', 'args': args}

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
    def dedup(columns=None, max_keys=None, sorted=False, max_state_bytes=None):
        """Deduplicate rows. Alias for unique. Use sorted=True for adjacent-key streaming mode."""
        args = {}
        if columns is not None:
            args['columns'] = columns
        if max_keys is not None:
            args['max_keys'] = max_keys
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if sorted:
            args['sorted'] = True
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

    @staticmethod
    def json_extract(path, result, column='_line', type='string'):
        """Extract a JSON Pointer or simple JSONPath value into a new column."""
        return {'op': 'json-extract', 'args': {
            'column': column,
            'path': path,
            'result': result,
            'type': type,
        }}

    @staticmethod
    def json_filter(path, op='exists', value=None, column='_line', type='auto'):
        """Filter rows by a JSON Pointer or simple JSONPath predicate."""
        args = {'column': column, 'path': path, 'op': op, 'type': type}
        if value is not None:
            args['value'] = value
        return {'op': 'json-filter', 'args': args}

    @staticmethod
    def json_schema(schema, column='_line', mode='annotate', result='_valid', audit=False, audit_limit=None):
        """Validate each JSON text cell against a supported JSON Schema subset."""
        args = {
            'column': column,
            'schema': schema,
            'mode': mode,
            'result': result,
        }
        if audit:
            args['audit'] = True
        if audit_limit is not None:
            args['audit_limit'] = audit_limit
        return {'op': 'json-schema', 'args': args}

    @staticmethod
    def json_flatten(fields, column='_line'):
        """Append declared JSON Pointer/simple JSONPath fields as columns."""
        return {'op': 'json-flatten', 'args': {
            'column': column,
            'fields': fields,
        }}

    @staticmethod
    def join(file, on, how='inner', max_lookup_rows=None, max_lookup_keys=None,
             max_lookup_bytes=None, max_state_bytes=None, max_matches_per_row=None,
             max_output_rows=None, sorted=False):
        """Join against a lookup CSV. Use sorted=True when both inputs are sorted by the join key."""
        args = {'file': file, 'on': on}
        if how != 'inner':
            args['how'] = how
        if max_lookup_rows is not None:
            args['max_lookup_rows'] = max_lookup_rows
        if max_lookup_keys is not None:
            args['max_lookup_keys'] = max_lookup_keys
        if max_lookup_bytes is not None:
            args['max_lookup_bytes'] = max_lookup_bytes
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if max_matches_per_row is not None:
            args['max_matches_per_row'] = max_matches_per_row
        if max_output_rows is not None:
            args['max_output_rows'] = max_output_rows
        if sorted:
            args['sorted'] = True
        return {'op': 'join', 'args': args}

    @staticmethod
    def semi_join(file, on, max_lookup_rows=None, max_lookup_keys=None,
                  max_lookup_bytes=None, max_state_bytes=None, max_output_rows=None, sorted=False):
        """Keep input rows whose key appears in a lookup CSV without adding lookup columns."""
        args = {'file': file, 'on': on}
        if max_lookup_rows is not None:
            args['max_lookup_rows'] = max_lookup_rows
        if max_lookup_keys is not None:
            args['max_lookup_keys'] = max_lookup_keys
        if max_lookup_bytes is not None:
            args['max_lookup_bytes'] = max_lookup_bytes
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if max_output_rows is not None:
            args['max_output_rows'] = max_output_rows
        if sorted:
            args['sorted'] = True
        return {'op': 'semi-join', 'args': args}

    @staticmethod
    def anti_join(file, on, max_lookup_rows=None, max_lookup_keys=None,
                  max_lookup_bytes=None, max_state_bytes=None, max_output_rows=None, sorted=False):
        """Keep input rows whose key is absent from a lookup CSV without adding lookup columns."""
        args = {'file': file, 'on': on}
        if max_lookup_rows is not None:
            args['max_lookup_rows'] = max_lookup_rows
        if max_lookup_keys is not None:
            args['max_lookup_keys'] = max_lookup_keys
        if max_lookup_bytes is not None:
            args['max_lookup_bytes'] = max_lookup_bytes
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if max_output_rows is not None:
            args['max_output_rows'] = max_output_rows
        if sorted:
            args['sorted'] = True
        return {'op': 'anti-join', 'args': args}

    @staticmethod
    def intersect(file, columns=None, max_lookup_rows=None, max_lookup_keys=None,
                  max_lookup_bytes=None, max_output_keys=None, max_state_bytes=None, sorted=False):
        """Keep distinct input rows whose row/key appears in a lookup CSV file."""
        args = {'file': file}
        if columns is not None:
            args['columns'] = columns
        if max_lookup_rows is not None:
            args['max_lookup_rows'] = max_lookup_rows
        if max_lookup_keys is not None:
            args['max_lookup_keys'] = max_lookup_keys
        if max_lookup_bytes is not None:
            args['max_lookup_bytes'] = max_lookup_bytes
        if max_output_keys is not None:
            args['max_output_keys'] = max_output_keys
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if sorted:
            args['sorted'] = True
        return {'op': 'intersect', 'args': args}

    @staticmethod
    def setdiff(file, columns=None, max_lookup_rows=None, max_lookup_keys=None,
                max_lookup_bytes=None, max_output_keys=None, max_state_bytes=None, sorted=False):
        """Keep distinct input rows whose row/key does not appear in a lookup CSV file."""
        args = {'file': file}
        if columns is not None:
            args['columns'] = columns
        if max_lookup_rows is not None:
            args['max_lookup_rows'] = max_lookup_rows
        if max_lookup_keys is not None:
            args['max_lookup_keys'] = max_lookup_keys
        if max_lookup_bytes is not None:
            args['max_lookup_bytes'] = max_lookup_bytes
        if max_output_keys is not None:
            args['max_output_keys'] = max_output_keys
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if sorted:
            args['sorted'] = True
        return {'op': 'setdiff', 'args': args}

    @staticmethod
    def intersect_all(file, columns=None, max_lookup_rows=None, max_lookup_keys=None,
                      max_lookup_bytes=None, max_state_bytes=None, sorted=False):
        """Keep up to lookup-count copies of each input row/key, preserving left order."""
        args = {'file': file}
        if columns is not None:
            args['columns'] = columns
        if max_lookup_rows is not None:
            args['max_lookup_rows'] = max_lookup_rows
        if max_lookup_keys is not None:
            args['max_lookup_keys'] = max_lookup_keys
        if max_lookup_bytes is not None:
            args['max_lookup_bytes'] = max_lookup_bytes
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if sorted:
            args['sorted'] = True
        return {'op': 'intersect-all', 'args': args}

    @staticmethod
    def setdiff_all(file, columns=None, max_lookup_rows=None, max_lookup_keys=None,
                    max_lookup_bytes=None, max_state_bytes=None, sorted=False):
        """Keep input row/key copies left after consuming matching lookup copies."""
        args = {'file': file}
        if columns is not None:
            args['columns'] = columns
        if max_lookup_rows is not None:
            args['max_lookup_rows'] = max_lookup_rows
        if max_lookup_keys is not None:
            args['max_lookup_keys'] = max_lookup_keys
        if max_lookup_bytes is not None:
            args['max_lookup_bytes'] = max_lookup_bytes
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if sorted:
            args['sorted'] = True
        return {'op': 'setdiff-all', 'args': args}

    @staticmethod
    def union(file, columns=None, max_lookup_rows=None, max_lookup_bytes=None,
              max_output_keys=None, max_state_bytes=None, sorted=False):
        """Append another CSV and keep the first distinct row/key across input and file."""
        args = {'file': file}
        if columns is not None:
            args['columns'] = columns
        if max_lookup_rows is not None:
            args['max_lookup_rows'] = max_lookup_rows
        if max_lookup_bytes is not None:
            args['max_lookup_bytes'] = max_lookup_bytes
        if max_output_keys is not None:
            args['max_output_keys'] = max_output_keys
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        if sorted:
            args['sorted'] = True
        return {'op': 'union', 'args': args}

    @staticmethod
    def union_all(file, max_lookup_rows=None, max_lookup_bytes=None):
        """Append every row from another CSV without duplicate elimination."""
        args = {'file': file}
        if max_lookup_rows is not None:
            args['max_lookup_rows'] = max_lookup_rows
        if max_lookup_bytes is not None:
            args['max_lookup_bytes'] = max_lookup_bytes
        return {'op': 'union-all', 'args': args}

    @staticmethod
    def stack(file, tag=None, tag_value=None):
        """Vertically concatenate another CSV file. Example: tf.ops.stack('other.csv', tag='source')"""
        args = {'file': file}
        if tag is not None:
            args['tag'] = tag
        if tag_value is not None:
            args['tag_value'] = tag_value
        return {'op': 'stack', 'args': args}

    @staticmethod
    def lead(column, offset=1, result=None):
        """Lookahead: access value N rows ahead. Example: tf.ops.lead('price', 1, 'next_price')"""
        args = {'column': column, 'offset': offset}
        if result is not None:
            args['result'] = result
        return {'op': 'lead', 'args': args}

    @staticmethod
    def lag(column, offset=1, result=None):
        """Previous-row shift. Example: tf.ops.lag('price', 1, 'prev_price')"""
        args = {'column': column, 'offset': offset}
        if result is not None:
            args['result'] = result
        return {'op': 'lag', 'args': args}

    @staticmethod
    def shift(column, offset=1, result=None, type='lag'):
        """Shift a column by rows. type='lag' is streaming; type='lead' delays output by offset rows."""
        args = {'column': column, 'offset': offset}
        if result is not None:
            args['result'] = result
        if type != 'lag':
            args['type'] = type
        return {'op': 'shift', 'args': args}

    @staticmethod
    def rowid(columns=None, result=None, sorted=False, max_keys=None, max_state_bytes=None):
        """1-based row id globally or within groups. Example: tf.ops.rowid(['city'], 'city_row', max_keys=10000)"""
        args = {}
        if columns is not None:
            args['columns'] = columns if isinstance(columns, list) else [columns]
        if result is not None:
            args['result'] = result
        if sorted:
            args['sorted'] = True
        if max_keys is not None:
            args['max_keys'] = max_keys
        if max_state_bytes is not None:
            args['max_state_bytes'] = max_state_bytes
        return {'op': 'rowid', 'args': args}

    @staticmethod
    def rleid(columns, result=None):
        """Consecutive run id by column(s). Example: tf.ops.rleid(['city'], 'run_id')"""
        args = {'columns': columns if isinstance(columns, list) else [columns]}
        if result is not None:
            args['result'] = result
        return {'op': 'rleid', 'args': args}

    @staticmethod
    def date_trunc(column, trunc, result=None):
        """Truncate date/timestamp to granularity. Example: tf.ops.date_trunc('ts', 'month')"""
        args = {'column': column, 'trunc': trunc}
        if result is not None:
            args['result'] = result
        return {'op': 'date-trunc', 'args': args}


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
