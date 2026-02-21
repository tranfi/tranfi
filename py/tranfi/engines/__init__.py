"""Engine registry for alternative pipeline executors."""

_engines = {}


def get_engine(name):
    """Get an engine instance by name."""
    if name in _engines:
        return _engines[name]

    if name == 'duckdb':
        from .duckdb_engine import DuckDBEngine
        _engines[name] = DuckDBEngine()
        return _engines[name]

    raise ValueError(f"Unknown engine: {name!r}. Available: 'native', 'duckdb'")
