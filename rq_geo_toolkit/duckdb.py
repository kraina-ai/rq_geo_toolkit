"""Helper functions for DuckDB."""

from pathlib import Path
from typing import Union

import duckdb


def sql_escape(value: str) -> str:
    """Escape value for SQL query."""
    return value.replace("'", "''")


def set_up_duckdb_connection(
    tmp_dir_path: Union[str, Path], preserve_insertion_order: bool = False
) -> "duckdb.DuckDBPyConnection":
    """Create DuckDB connection in a given directory."""
    local_db_file = "db.duckdb"
    connection = duckdb.connect(
        database=str(Path(tmp_dir_path) / local_db_file),
        config=dict(preserve_insertion_order=preserve_insertion_order),
    )
    connection.sql("SET enable_progress_bar = false;")
    connection.sql("SET enable_progress_bar_print = false;")

    connection.install_extension("spatial")
    connection.load_extension("spatial")

    return connection
