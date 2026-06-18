"""DuckDB engine — executes tranfi pipelines via SQL transpilation."""

import os
import tempfile
import math
from .._ffi import compile_to_sql
from ..pipeline import PipelineResult
from ..memory_policy import memory_for_engine

try:
    import numpy as _np
    _FLOAT_TYPES = (float, _np.floating)
except ImportError:
    _FLOAT_TYPES = (float,)


def _sql_string(value):
    return str(value).replace("'", "''")


def _format_timestamp(value):
    base = value.strftime('%Y-%m-%dT%H:%M:%S')
    microsecond = getattr(value, 'microsecond', 0)
    if microsecond:
        return base + '.' + f'{microsecond:06d}'.rstrip('0') + 'Z'
    return base + 'Z'


def _format_float(value):
    numeric = float(value)
    if not math.isfinite(numeric):
        return str(numeric)
    return format(numeric, '.17g')


def _normalize_duckdb_dataframe(df, description):
    """Format DuckDB result columns like Tranfi's CSV encoder."""
    try:
        import pandas as pd
    except ImportError:
        return df

    for idx, column in enumerate(df.columns):
        duck_type = ''
        if description and idx < len(description):
            duck_type = str(description[idx][1]).upper()

        if duck_type == 'BOOLEAN':
            df[column] = df[column].map(
                lambda value: '' if pd.isna(value) else ('true' if bool(value) else 'false')
            )
        elif duck_type == 'DATE':
            values = pd.to_datetime(df[column], errors='coerce')
            df[column] = values.map(
                lambda value: '' if pd.isna(value) else value.strftime('%Y-%m-%d')
            )
        elif duck_type.startswith('TIMESTAMP'):
            values = pd.to_datetime(df[column], errors='coerce', utc=True)
            df[column] = values.map(
                lambda value: '' if pd.isna(value) else _format_timestamp(value)
            )
    return df


def _csv_escape(text):
    if any(ch in text for ch in ',\"\r\n'):
        return '"' + text.replace('"', '""') + '"'
    return text


def _dataframe_to_tranfi_csv(df):
    """Serialize DuckDB results with Tranfi CSV null/boolean/date formatting."""
    try:
        import pandas as pd
    except ImportError:
        pd = None

    def field(value):
        if value is None:
            return ''
        if pd is not None and pd.isna(value):
            return ''
        if isinstance(value, _FLOAT_TYPES):
            return _csv_escape(_format_float(value))
        return _csv_escape(str(value))

    lines = [','.join(_csv_escape(str(column)) for column in df.columns)]
    for row in df.itertuples(index=False, name=None):
        lines.append(','.join(field(value) for value in row))
    return '\n'.join(lines) + '\n'


class DuckDBEngine:
    """Execute tranfi pipelines using DuckDB."""

    def run(self, dsl, *, input=None, input_file=None, memory=None, spill_dir=None):
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "DuckDB engine requires the 'duckdb' package. "
                "Install it with: pip install tranfi[duckdb]"
            )

        sql = compile_to_sql(dsl)
        conn = duckdb.connect(':memory:')

        memory_limit = memory_for_engine(memory)
        if memory_limit:
            conn.execute(f"SET memory_limit = '{_sql_string(memory_limit)}'")
        if spill_dir:
            conn.execute(f"SET temp_directory = '{_sql_string(spill_dir)}'")

        tmp_path = None
        try:
            if input_file:
                sql = sql.replace('input_data', f"read_csv('{_sql_string(input_file)}')")
            elif input is not None:
                fd, tmp_path = tempfile.mkstemp(suffix='.csv')
                os.write(fd, input)
                os.close(fd)
                sql = sql.replace('input_data', f"read_csv('{_sql_string(tmp_path)}')")
            else:
                raise ValueError("Either input or input_file must be provided")

            result = conn.execute(sql)
            description = result.description
            df = _normalize_duckdb_dataframe(result.fetchdf(), description)
            output = _dataframe_to_tranfi_csv(df).encode('utf-8')
        finally:
            conn.close()
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

        return PipelineResult(output=output, errors=b'', stats=b'', samples=b'')
