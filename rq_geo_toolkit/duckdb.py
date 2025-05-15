"""Helper functions for DuckDB."""

import multiprocessing
import tempfile
import time
from pathlib import Path
from time import sleep
from typing import Optional, Union

import duckdb
import psutil

from rq_geo_toolkit.constants import MEMORY_1GB


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


def run_query_with_memory_monitoring(
    sql_query: str,
    tmp_dir_path: Path,
    preserve_insertion_order: bool = False,
    memory_percentage_threshold: float = 0.95,
    query_timeout_seconds: Optional[int] = None,
) -> None:
    """Run SQL query and raise exception if memory threshold is exceeded."""
    assert 0 < memory_percentage_threshold <= 1

    with multiprocessing.get_context("spawn").Pool() as pool:
        r = pool.apply_async(_run_query, args=(sql_query, tmp_dir_path, preserve_insertion_order))
        start_time = time.time()
        actual_memory = psutil.virtual_memory()
        percentage_threshold = 100 * memory_percentage_threshold
        if (actual_memory.total * 0.05) > MEMORY_1GB:
            percentage_threshold = 100 * (actual_memory.total - MEMORY_1GB) / actual_memory.total
        while not r.ready():
            actual_memory = psutil.virtual_memory()
            if actual_memory.percent > percentage_threshold:
                raise MemoryError()

            current_time = time.time()
            elapsed_seconds = current_time - start_time
            if query_timeout_seconds is not None and elapsed_seconds > query_timeout_seconds:
                raise TimeoutError()

            sleep(0.5)
        r.get()


def _run_query(sql_query: str, tmp_dir_path: Path, preserve_insertion_order: bool) -> None:
    with (
        tempfile.TemporaryDirectory(dir=tmp_dir_path) as tmp_dir_name,
        set_up_duckdb_connection(
            tmp_dir_path=Path(tmp_dir_name), preserve_insertion_order=preserve_insertion_order
        ) as conn,
    ):
        conn.sql(sql_query)
