"""Helper functions for DuckDB."""

import tempfile
from collections.abc import Callable
from functools import partial
from math import ceil
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Any, Optional, Union

import duckdb
import psutil
from rich import print as rprint

from rq_geo_toolkit.constants import (
    MEMORY_1GB,
)
from rq_geo_toolkit.multiprocessing_utils import WorkerProcess

if TYPE_CHECKING:  # pragma: no cover
    from rq_geo_toolkit.rich_utils import VERBOSITY_MODE

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


def run_duckdb_query_function_with_memory_limit(
    tmp_dir_path: Path,
    verbosity_mode: "VERBOSITY_MODE",
    current_memory_gb_limit: Optional[float],
    current_threads_limit: Optional[int],
    function: Callable[..., None],
    args: Any,
) -> tuple[float, int]:
    """Run function with duckdb query and limit threads automatically."""
    current_memory_gb_limit = current_memory_gb_limit or ceil(
        psutil.virtual_memory().total / MEMORY_1GB
    )
    current_threads_limit = (
        current_threads_limit
        or duckdb.sql("SELECT current_setting('threads') AS threads").fetchone()[0]
    )

    while current_threads_limit > 1:
        try:
            with tempfile.TemporaryDirectory(dir=Path(tmp_dir_path).resolve()) as tmp_dir_name:
                nested_tmp_dir_path = Path(tmp_dir_name)
                sleep(0.5)
                f = partial(
                    function,
                    current_memory_gb_limit=current_memory_gb_limit,
                    current_threads_limit=current_threads_limit,
                    tmp_dir_path=nested_tmp_dir_path,
                )
                process = WorkerProcess(
                    target=f,
                    args=args,
                )
                process.start()
                actual_memory = psutil.virtual_memory()
                percentage_threshold = 95
                if (actual_memory.total * 0.05) > MEMORY_1GB:
                    percentage_threshold = (
                        100 * (actual_memory.total - MEMORY_1GB) / actual_memory.total
                    )
                while process.is_alive():
                    actual_memory = psutil.virtual_memory()
                    if actual_memory.percent > percentage_threshold:
                        raise MemoryError()

                    sleep(0.5)

                if process.exception:
                    error, traceback = process.exception
                    msg = f"{error}\n\nOriginal {traceback}"
                    raise type(error)(msg)

                if process.exitcode != 0:
                    raise MemoryError()

            return current_memory_gb_limit, current_threads_limit
        except (duckdb.OutOfMemoryException, MemoryError) as ex:
            if current_threads_limit == 1 and current_memory_gb_limit < 1:
                raise RuntimeError(
                    "Not enough memory to run the query."
                ) from ex
            elif current_threads_limit > 1:
                # First limit number of CPUs
                current_threads_limit = ceil(current_threads_limit / 2)
            elif current_memory_gb_limit > 1:
                # Next limit memory
                current_memory_gb_limit = ceil(current_memory_gb_limit / 2)
            elif current_memory_gb_limit == 1:
                # Reduce memory below 1 GB
                current_memory_gb_limit /= 2

            if not verbosity_mode == "silent":
                rprint(
                    f"Encountered {ex.__class__.__name__} during operation."
                    " Retrying with lower number of resources"
                    f" ({current_memory_gb_limit:.2f}GB, {current_threads_limit} threads)."
                )

    raise RuntimeError("Not enough memory to run the query.")


def run_query_with_memory_monitoring(
    sql_query: str,
    tmp_dir_path: Path,
    verbosity_mode: "VERBOSITY_MODE",
    preserve_insertion_order: bool = False,
) -> None:
    """Run SQL query and raise exception if memory threshold is exceeded."""
    run_duckdb_query_function_with_memory_limit(
        tmp_dir_path=tmp_dir_path,
        verbosity_mode=verbosity_mode,
        current_memory_gb_limit=None,
        current_threads_limit=None,
        function=_run_query,
        args=(sql_query, preserve_insertion_order),
    )


def _run_query(
    sql_query: str,
    preserve_insertion_order: bool,
    current_memory_gb_limit: float,
    current_threads_limit: int,
    tmp_dir_path: Path,
) -> None:
    with (
        tempfile.TemporaryDirectory(dir=tmp_dir_path) as tmp_dir_name,
        set_up_duckdb_connection(
            tmp_dir_path=Path(tmp_dir_name), preserve_insertion_order=preserve_insertion_order
        ) as conn,
    ):
        conn.execute(f"SET memory_limit = '{current_memory_gb_limit}GB';")
        conn.execute(f"SET threads = {current_threads_limit};")
        conn.sql(sql_query)
