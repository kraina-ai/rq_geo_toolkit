"""Module for sorting GeoParquet files."""

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

import pyarrow.parquet as pq
from duckdb import OutOfMemoryException
from rich import print as rprint

from rq_geo_toolkit.constants import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    PARQUET_ROW_GROUP_SIZE,
)
from rq_geo_toolkit.duckdb import run_query_with_memory_monitoring, set_up_duckdb_connection
from rq_geo_toolkit.geoparquet_compression import (
    compress_parquet_with_duckdb,
)

if TYPE_CHECKING:  # pragma: no cover
    from rq_geo_toolkit.rich_utils import VERBOSITY_MODE


def sort_geoparquet_file_by_geometry(
    input_file_path: Path,
    output_file_path: Optional[Path] = None,
    sort_extent: Optional[tuple[float, float, float, float]] = None,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    working_directory: Union[str, Path] = "files",
    verbosity_mode: "VERBOSITY_MODE" = "transient",
    remove_input_file: bool = True,
) -> Path:
    """
    Sorts a GeoParquet file by the geometry column.

    Args:
        input_file_path (Path): Input GeoParquet file path.
        output_file_path (Optional[Path], optional): Output GeoParquet file path.
            If not provided, will generate file name based on input file name with
            `_sorted` suffix. Defaults to None.
        sort_extent (Optional[tuple[float, float, float, float]], optional): Extent to use
            in the ST_Hilbert function. If not, will calculate extent from the
            geometries in the file. Defaults to None.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        working_directory (Union[str, Path], optional): Directory where to save
            the downloaded `*.parquet` files. Defaults to "files".
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        remove_input_file (bool, optional): Remove the original file after sorting.
            Defaults to True.
    """
    if output_file_path is None:
        output_file_path = (
            input_file_path.parent / f"{input_file_path.stem}_sorted{input_file_path.suffix}"
        )

    assert input_file_path.resolve().as_posix() != output_file_path.resolve().as_posix()

    if pq.read_metadata(input_file_path).num_rows == 0:
        return input_file_path.rename(output_file_path)

    Path(working_directory).mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(dir=Path(working_directory).resolve()) as tmp_dir_name:
        tmp_dir_path = Path(tmp_dir_name)

        order_dir_path = tmp_dir_path / "ordered"
        order_dir_path.mkdir(parents=True, exist_ok=True)

        _sort_with_duckdb(
            input_file_path=input_file_path,
            output_dir_path=order_dir_path,
            sort_extent=sort_extent,
            tmp_dir_path=tmp_dir_path,
            verbosity_mode=verbosity_mode,
        )

        original_metadata = pq.read_metadata(input_file_path)

        if remove_input_file:
            input_file_path.unlink()

        order_files = sorted(order_dir_path.glob("*.parquet"), key=lambda x: int(x.stem))

        compress_parquet_with_duckdb(
            input_file_path=order_files,
            output_file_path=output_file_path,
            compression=compression,
            compression_level=compression_level,
            row_group_size=row_group_size,
            working_directory=tmp_dir_path,
            parquet_metadata=original_metadata,
            verbosity_mode=verbosity_mode,
        )

    return output_file_path


def _sort_with_duckdb(
    input_file_path: Path,
    output_dir_path: Path,
    sort_extent: Optional[tuple[float, float, float, float]],
    tmp_dir_path: Path,
    verbosity_mode: "VERBOSITY_MODE",
) -> None:
    connection = set_up_duckdb_connection(tmp_dir_path, preserve_insertion_order=True)

    struct_type = "::STRUCT(min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE)"
    connection.sql(
        f"""
        CREATE OR REPLACE MACRO bbox_within(a, b) AS
        (
            (a{struct_type}).min_x >= (b{struct_type}).min_x and
            (a{struct_type}).max_x <= (b{struct_type}).max_x
        )
        and
        (
            (a{struct_type}).min_y >= (b{struct_type}).min_y and
            (a{struct_type}).max_y <= (b{struct_type}).max_y
        );
        """
    )

    # https://medium.com/radiant-earth-insights/using-duckdbs-hilbert-function-with-geop-8ebc9137fb8a
    if sort_extent is None:
        # Calculate extent from the geometries in the file
        order_clause = f"""
        ST_Hilbert(
            geometry,
            (
                SELECT ST_Extent(ST_Extent_Agg(geometry))::BOX_2D
                FROM read_parquet('{input_file_path}', hive_partitioning=false)
            )
        )
        """
    else:
        extent_box_clause = f"""
        {{
            min_x: {sort_extent[0]},
            min_y: {sort_extent[1]},
            max_x: {sort_extent[2]},
            max_y: {sort_extent[3]}
        }}::BOX_2D
        """
        # Keep geometries within the extent first,
        # and geometries that are bigger than the extent last (like administrative boundaries)

        # Then sort by Hilbert curve but readjust the extent to all geometries that
        # are not fully within the extent, but also not bigger than the extent overall.
        order_clause = f"""
        bbox_within(({extent_box_clause}), ST_Extent(geometry)),
        ST_Hilbert(
            geometry,
            (
                SELECT ST_Extent(ST_Extent_Agg(geometry))::BOX_2D
                FROM read_parquet('{input_file_path}', hive_partitioning=false)
                WHERE NOT bbox_within(({extent_box_clause}), ST_Extent(geometry))
            )
        )
        """

    relation = connection.sql(
        f"""
        SELECT file_row_number, row_number() OVER (ORDER BY {order_clause}) as order_id
        FROM read_parquet('{input_file_path}', hive_partitioning=false, file_row_number=true)
        """
    )

    index_file_path = tmp_dir_path / "order_index.parquet"
    relation.to_parquet(str(index_file_path), compression="zstd")

    total_rows = connection.read_parquet(str(index_file_path)).count("*").fetchone()[0]
    connection.close()

    current_file_idx = 0
    current_offset = 0
    current_limit = 10_000_000

    while current_offset < total_rows:
        try:
            sql_query = f"""
            COPY (
                WITH order_batch AS (
                    FROM read_parquet('{index_file_path}')
                    LIMIT {current_limit} OFFSET {current_offset}
                )
                SELECT input_data.* EXCLUDE (file_row_number)
                FROM order_batch
                JOIN read_parquet(
                    '{input_file_path}',
                    hive_partitioning=false,
                    file_row_number=true
                ) input_data USING (file_row_number)
                ORDER BY order_id
            ) TO '{output_dir_path}/{current_file_idx}.parquet' (
                FORMAT 'parquet'
            )
            """
            run_query_with_memory_monitoring(
                sql_query=sql_query, tmp_dir_path=tmp_dir_path, preserve_insertion_order=True
            )

            current_file_idx += 1
            current_offset += current_limit
        except (OutOfMemoryException, MemoryError) as ex:
            current_limit //= 10
            if current_limit == 1:
                raise

            if not verbosity_mode == "silent":
                rprint(
                    f"Encountered {ex.__class__.__name__} during operation."
                    f" Retrying with lower number of rows per batch ({current_limit} rows)."
                )
