"""DuckDB engine — executes tranfi pipelines via SQL transpilation."""

import os
import tempfile
from .._ffi import compile_to_sql
from ..pipeline import PipelineResult
from ..memory_policy import memory_for_engine


def _sql_string(value):
    return str(value).replace("'", "''")


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
                sql = sql.replace('input_data', f"read_csv('{input_file}')")
            elif input is not None:
                fd, tmp_path = tempfile.mkstemp(suffix='.csv')
                os.write(fd, input)
                os.close(fd)
                sql = sql.replace('input_data', f"read_csv('{tmp_path}')")
            else:
                raise ValueError("Either input or input_file must be provided")

            result = conn.execute(sql)
            df = result.fetchdf()
            output = df.to_csv(index=False).encode('utf-8')
        finally:
            conn.close()
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

        return PipelineResult(output=output, errors=b'', stats=b'', samples=b'')
