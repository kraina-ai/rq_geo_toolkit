"""Module for compressing GeoParquet files."""

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Optional, Union, cast

import pyarrow.parquet as pq

from rq_geo_toolkit.constants import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    PARQUET_ROW_GROUP_SIZE,
    PARQUET_VERSION,
)
from rq_geo_toolkit.duckdb import (
    DUCKDB_ABOVE_130,
    DuckDBConnKwargs,
    run_duckdb_query_function_with_memory_limit,
    set_up_duckdb_connection,
    sql_escape,
)

if TYPE_CHECKING:  # pragma: no cover
    from rq_geo_toolkit.rich_utils import VERBOSITY_MODE


def compress_parquet_with_duckdb(
    input_file_path: Union[Path, list[Path]],
    output_file_path: Path,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    working_directory: Union[str, Path] = "files",
    parquet_metadata: Optional[pq.FileMetaData] = None,
    verbosity_mode: "VERBOSITY_MODE" = "transient",
    duckdb_conn_kwargs: Optional[DuckDBConnKwargs] = None,
) -> Path:
    """
    Compresses a GeoParquet file while keeping its metadata.

    Args:
        input_file_path (Union[Path, list[Path]]): Input GeoParquet file path (or paths).
        output_file_path (Path): Output GeoParquet file path.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Supported only for zstd compression. Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        parquet_version (Literal["v1", "v2"], optional): What type of parquet version use to
            save final file. Defaults to "v2".
        working_directory (Union[str, Path], optional): Directory where to save
            the downloaded `*.parquet` files. Defaults to "files".
        parquet_metadata (Optional[pq.FileMetaData], optional): GeoParquet file metadata used to
            copy. If not provided, will load the metadata from the input file. Defaults to None.
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        duckdb_conn_kwargs (Optional[DuckDBConnKwargs], optional): Additional kwargs used to
            provision a duckdb connection. Defaults to None.
    """
    is_single_path = isinstance(input_file_path, Path)
    if is_single_path:
        assert (
            cast("Path", input_file_path).resolve().as_posix()
            != output_file_path.resolve().as_posix()
        )

    Path(working_directory).mkdir(parents=True, exist_ok=True)

    if is_single_path and pq.read_metadata(input_file_path).num_rows == 0:
        return cast("Path", input_file_path).rename(output_file_path)

    if is_single_path:
        sql_input_str = f"'{input_file_path}'"
    else:
        mapped_paths = ", ".join(f"'{path}'" for path in cast("list[Path]", input_file_path))
        sql_input_str = f"[{mapped_paths}]"

    parquet_metadata = parquet_metadata or pq.read_metadata(input_file_path)

    query = f"""
    SELECT original_data.*
    FROM read_parquet({sql_input_str}, hive_partitioning=false) original_data
    """

    return compress_query_with_duckdb(
        query=query,
        parquet_metadata=parquet_metadata,
        output_file_path=output_file_path,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        parquet_version=parquet_version,
        working_directory=working_directory,
        verbosity_mode=verbosity_mode,
        duckdb_conn_kwargs=duckdb_conn_kwargs,
    )


def compress_query_with_duckdb(
    query: str,
    parquet_metadata: pq.FileMetaData,
    output_file_path: Path,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    working_directory: Union[str, Path] = "files",
    verbosity_mode: "VERBOSITY_MODE" = "transient",
    duckdb_conn_kwargs: Optional[DuckDBConnKwargs] = None,
) -> Path:
    """
    Compresses query to a GeoParquet file while keeping its metadata.

    Args:
        query (str): Input DuckDB query to compress.
        output_file_path (Path): Output GeoParquet file path.
        parquet_metadata (pq.FileMetaData): GeoParquet file metadata used to
            copy. If not provided, will load the metadata from the input file. Defaults to None.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Supported only for zstd compression. Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        parquet_version (Literal["v1", "v2"], optional): What type of parquet version use to
            save final file. Defaults to "v2".
        working_directory (Union[str, Path], optional): Directory where to save
            the downloaded `*.parquet` files. Defaults to "files".
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        duckdb_conn_kwargs (Optional[DuckDBConnKwargs], optional): Additional kwargs used to
            provision a duckdb connection. Defaults to None.
    """
    with tempfile.TemporaryDirectory(dir=Path(working_directory).resolve()) as tmp_dir_name:
        tmp_dir_path = Path(tmp_dir_name)

        original_metadata_string = _parquet_schema_metadata_to_duckdb_kv_metadata(parquet_metadata)

        run_duckdb_query_function_with_memory_limit(
            tmp_dir_path=tmp_dir_path,
            verbosity_mode=verbosity_mode,
            current_memory_gb_limit=None,
            current_threads_limit=None,
            function=_compress_with_memory_limit,
            kwargs=dict(
                query=query,
                output_file_path=output_file_path,
                original_metadata_string=original_metadata_string,
                compression=compression,
                compression_level=compression_level,
                row_group_size=row_group_size,
                parquet_version=parquet_version,
            ),
            duckdb_conn_kwargs=duckdb_conn_kwargs,
        )

    return output_file_path


def _compress_with_memory_limit(
    query: str,
    output_file_path: Path,
    original_metadata_string: str,
    compression: str,
    compression_level: int,
    row_group_size: int,
    parquet_version: str,
    current_memory_gb_limit: float,
    current_threads_limit: int,
    tmp_dir_path: Path,
    duckdb_conn_kwargs: Optional[DuckDBConnKwargs] = None,
) -> None:
    duckdb_conn_kwargs = duckdb_conn_kwargs or {}
    duckdb_conn_kwargs["provisioning_queries"] = [
        *duckdb_conn_kwargs.get("provisioning_queries", []),
        "SET enable_geoparquet_conversion = false;",
        f"SET memory_limit = '{current_memory_gb_limit}GB';",
        f"SET threads = {current_threads_limit};",
    ]
    connection = set_up_duckdb_connection(
        tmp_dir_path,
        preserve_insertion_order=True,
        duckdb_conn_kwargs=duckdb_conn_kwargs,
    )

    parquet_version_query = f"PARQUET_VERSION {parquet_version}," if DUCKDB_ABOVE_130 else ""
    compression_level_query = (
        f"COMPRESSION_LEVEL {compression_level}," if compression == "zstd" else ""
    )
    connection.execute(
        f"""
        COPY ({query}) TO '{output_file_path}' (
            FORMAT parquet,
            {parquet_version_query}
            COMPRESSION {compression},
            {compression_level_query}
            ROW_GROUP_SIZE {row_group_size},
            KV_METADATA {original_metadata_string}
        );
        """
    )

    print("stopping conn")
    connection.close()


def _parquet_schema_metadata_to_duckdb_kv_metadata(
    parquet_file_metadata: pq.FileMetaData,
) -> str:
    kv_pairs = []
    for key, value in parquet_file_metadata.metadata.items():
        escaped_key = sql_escape(key.decode())
        escaped_value = sql_escape(value.decode())
        kv_pairs.append(f"'{escaped_key}': '{escaped_value}'")

    return "{ " + ", ".join(kv_pairs) + " }"
