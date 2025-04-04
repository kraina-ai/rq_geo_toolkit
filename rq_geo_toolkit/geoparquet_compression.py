"""Module for sorting GeoParquet files."""

import multiprocessing
import tempfile
from collections.abc import Callable
from functools import partial
from math import ceil
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Any, Optional, Union

import duckdb
import psutil
import pyarrow.parquet as pq
from rich import print as rprint

from rq_geo_toolkit.constants import PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL, PARQUET_ROW_GROUP_SIZE
from rq_geo_toolkit.duckdb import set_up_duckdb_connection

if TYPE_CHECKING:  # pragma: no cover
    from rq_geo_toolkit.rich_utils import VERBOSITY_MODE

MEMORY_1GB = 1024**3


def compress_parquet_with_duckdb(
    input_file_path: Path,
    output_file_path: Path,
    working_directory: Union[str, Path] = "files",
    parquet_metadata: Optional[pq.FileMetaData] = None,
    verbosity_mode: "VERBOSITY_MODE" = "transient",
) -> Path:
    """Compresses a GeoParquet file while keeping its metadata.

    Args:
        input_file_path (Path): Input GeoParquet file path.
        output_file_path (Path): Output GeoParquet file path.
        working_directory (Union[str, Path], optional): Directory where to save
            the downloaded `*.parquet` files. Defaults to "files".
        parquet_metadata (Optional[pq.FileMetaData], optional): GeoParquet file metadata used to
            copy. If not provided, will load the metadata from the input file. Defaults to None.
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
    """
    assert input_file_path.resolve().as_posix() != output_file_path.resolve().as_posix()

    Path(working_directory).mkdir(parents=True, exist_ok=True)

    if pq.read_metadata(input_file_path).num_rows == 0:
        return input_file_path.rename(output_file_path)

    with tempfile.TemporaryDirectory(dir=Path(working_directory).resolve()) as tmp_dir_name:
        tmp_dir_path = Path(tmp_dir_name)

        original_metadata_string = _parquet_schema_metadata_to_duckdb_kv_metadata(
            parquet_metadata or pq.read_metadata(input_file_path)
        )

        _run_query_with_memory_limit(
            tmp_dir_path=tmp_dir_path,
            verbosity_mode=verbosity_mode,
            current_memory_gb_limit=None,
            current_threads_limit=None,
            function=_compress_with_memory_limit,
            args=(input_file_path, output_file_path, original_metadata_string),
        )

    return output_file_path


def _compress_with_memory_limit(
    input_file_path: Union[list[Path], Path],
    output_file_path: Path,
    original_metadata_string: str,
    current_memory_gb_limit: float,
    current_threads_limit: int,
    tmp_dir_path: Path,
) -> None:
    connection = set_up_duckdb_connection(tmp_dir_path, preserve_insertion_order=True)

    connection.execute("SET enable_geoparquet_conversion = false;")
    connection.execute(f"SET memory_limit = '{current_memory_gb_limit}GB';")
    connection.execute(f"SET threads = {current_threads_limit};")

    if isinstance(input_file_path, Path):
        sql_input_str = f"'{input_file_path}'"
    else:
        mapped_paths = ", ".join(f"'{path}'" for path in input_file_path)
        sql_input_str = f"[{mapped_paths}]"

    connection.execute(
        f"""
        COPY (
            SELECT original_data.*
            FROM read_parquet({sql_input_str}, hive_partitioning=false) original_data
        ) TO '{output_file_path}' (
            FORMAT parquet,
            COMPRESSION {PARQUET_COMPRESSION},
            COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL},
            ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE},
            KV_METADATA {original_metadata_string}
        );
        """
    )

    connection.close()


def _run_query_with_memory_limit(
    tmp_dir_path: Path,
    verbosity_mode: "VERBOSITY_MODE",
    current_memory_gb_limit: Optional[float],
    current_threads_limit: Optional[int],
    function: Callable[..., None],
    args: Any,
) -> tuple[float, int]:
    current_memory_gb_limit = current_memory_gb_limit or ceil(
        psutil.virtual_memory().total / MEMORY_1GB
    )
    current_threads_limit = current_threads_limit or multiprocessing.cpu_count()

    while current_memory_gb_limit > 0:
        try:
            with (
                tempfile.TemporaryDirectory(dir=Path(tmp_dir_path).resolve()) as tmp_dir_name,
                multiprocessing.get_context("spawn").Pool() as pool,
            ):
                nested_tmp_dir_path = Path(tmp_dir_name)
                r = pool.apply_async(
                    func=partial(
                        function,
                        current_memory_gb_limit=current_memory_gb_limit,
                        current_threads_limit=current_threads_limit,
                        tmp_dir_path=nested_tmp_dir_path,
                    ),
                    args=args,
                )
                actual_memory = psutil.virtual_memory()
                percentage_threshold = 95
                if (actual_memory.total * 0.05) > MEMORY_1GB:
                    percentage_threshold = (
                        100 * (actual_memory.total - MEMORY_1GB) / actual_memory.total
                    )
                while not r.ready():
                    actual_memory = psutil.virtual_memory()
                    if actual_memory.percent > percentage_threshold:
                        raise MemoryError()

                    sleep(0.5)
                r.get()
            return current_memory_gb_limit, current_threads_limit
        except (duckdb.OutOfMemoryException, MemoryError) as ex:
            if current_memory_gb_limit < 1:
                raise RuntimeError(
                    "Not enough memory to run the ordering query. Please rerun without sorting."
                ) from ex

            if current_memory_gb_limit == 1:
                current_memory_gb_limit /= 2
            else:
                current_memory_gb_limit = ceil(current_memory_gb_limit / 2)

            current_threads_limit = ceil(current_threads_limit / 2)

            if not verbosity_mode == "silent":
                rprint(
                    f"Encountered {ex.__class__.__name__} during operation."
                    " Retrying with lower number of resources"
                    f" ({current_memory_gb_limit:.2f}GB, {current_threads_limit} threads)."
                )

    raise RuntimeError("Not enough memory to run the query. Please rerun without sorting.")


def _parquet_schema_metadata_to_duckdb_kv_metadata(parquet_file_metadata: pq.FileMetaData) -> str:
    def escape_single_quotes(s: str) -> str:
        return s.replace("'", "''")

    kv_pairs = []
    for key, value in parquet_file_metadata.metadata.items():
        escaped_key = escape_single_quotes(key.decode())
        escaped_value = escape_single_quotes(value.decode())
        kv_pairs.append(f"'{escaped_key}': '{escaped_value}'")

    return "{ " + ", ".join(kv_pairs) + " }"
