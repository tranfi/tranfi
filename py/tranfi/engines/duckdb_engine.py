"""DuckDB engine — executes tranfi pipelines via SQL transpilation."""

import os
import tempfile
from .._ffi import compile_to_sql
from ..pipeline import PipelineResult


class DuckDBEngine:
    """Execute tranfi pipelines using DuckDB."""

    def run(self, dsl, *, input=None, input_file=None):
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "DuckDB engine requires the 'duckdb' package. "
                "Install it with: pip install tranfi[duckdb]"
            )

        sql = compile_to_sql(dsl)
        conn = duckdb.connect(':memory:')

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
