"""Module for sorting GeoParquet files."""

import math
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Optional, Union

import pyarrow.parquet as pq
from duckdb import OutOfMemoryException
from rich import print as rprint

from rq_geo_toolkit.constants import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    PARQUET_ROW_GROUP_SIZE,
    PARQUET_VERSION,
)
from rq_geo_toolkit.duckdb import (
    DuckDBConnKwargs,
    run_query_with_memory_monitoring,
    set_up_duckdb_connection,
)
from rq_geo_toolkit.geoparquet_compression import compress_parquet_with_duckdb

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    import duckdb

    from rq_geo_toolkit.rich_utils import VERBOSITY_MODE


def sort_geoparquet_file_by_geometry(
    input_file_path: Path,
    output_file_path: Optional[Path] = None,
    sort_algorithm: Literal["str", "hilbert"] = "str",
    sort_extent: Optional[tuple[float, float, float, float]] = None,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    working_directory: Union[str, Path] = "files",
    verbosity_mode: "VERBOSITY_MODE" = "transient",
    remove_input_file: bool = True,
    progress_callback: Optional["Callable[[int], None]"] = None,
    duckdb_conn_kwargs: Optional[DuckDBConnKwargs] = None,
) -> Path:
    """
    Sorts a GeoParquet file by the geometry column.

    Args:
        input_file_path (Path): Input GeoParquet file path.
        output_file_path (Optional[Path], optional): Output GeoParquet file path.
            If not provided, will generate file name based on input file name with
            `_sorted` suffix. Defaults to None.
        sort_algorithm (Literal["str", "hilbert"], optional): Algorithm used to generate the
            ordering index. "str" uses Sort-Tile-Recursive packing (sorts geometries into
            vertical strips by centroid X, then orders each strip by centroid Y in a serpentine
            pattern), which produces row groups with low bounding-box overlap and is tuned to the
            target `row_group_size`. "hilbert" orders geometries along a Hilbert space-filling
            curve. Defaults to "str".
        sort_extent (Optional[tuple[float, float, float, float]], optional): Extent to use
            in the ST_Hilbert function. If not, will calculate extent from the
            geometries in the file. Only used by the "hilbert" algorithm; ignored for "str".
            Defaults to None.
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
        remove_input_file (bool, optional): Remove the original file after sorting.
            Defaults to True.
        progress_callback (Callable[[int], None], optional): A callback for reporting sorting
            progress. Will report current progress.
        duckdb_conn_kwargs (Optional[DuckDBConnKwargs], optional): Additional kwargs used to
            provision a duckdb connection. Defaults to None.
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
            sort_algorithm=sort_algorithm,
            sort_extent=sort_extent,
            row_group_size=row_group_size,
            tmp_dir_path=tmp_dir_path,
            verbosity_mode=verbosity_mode,
            progress_callback=progress_callback,
            duckdb_conn_kwargs=duckdb_conn_kwargs,
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
            parquet_version=parquet_version,
            working_directory=tmp_dir_path,
            parquet_metadata=original_metadata,
            verbosity_mode=verbosity_mode,
            duckdb_conn_kwargs=duckdb_conn_kwargs,
        )

    return output_file_path


def _hilbert_index_select_sql(
    input_file_path: Path,
    sort_extent: Optional[tuple[float, float, float, float]],
    connection: "duckdb.DuckDBPyConnection",
) -> str:
    """Build a SELECT producing (file_row_number, order_id) ordered by a Hilbert curve."""
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

    return f"""
        SELECT file_row_number, row_number() OVER (ORDER BY {order_clause}) as order_id
        FROM read_parquet('{input_file_path}', hive_partitioning=false, file_row_number=true)
        """


def _str_index_select_sql(
    input_file_path: Path,
    row_group_size: int,
    connection: "duckdb.DuckDBPyConnection",
) -> str:
    """
    Build a SELECT producing (file_row_number, order_id) using Sort-Tile-Recursive packing.

    STR splits the dataset into vertical strips based on geometry centroid X, then orders each
    strip by centroid Y in a serpentine (boustrophedon) pattern. The strip count is derived from
    the target `row_group_size` so that each resulting row group holds roughly that many rows with
    a tight, low-overlap bounding box - which improves spatial pruning during reads.

    Reference: https://github.com/Kanahiro/spatial-sort-benchmark
    """
    total_rows = connection.execute(
        f"SELECT count(*) FROM read_parquet('{input_file_path}', hive_partitioning=false)"
    ).fetchone()[0]

    tile_count = max(1, math.ceil(total_rows / row_group_size))
    strip_count = max(1, math.ceil(math.sqrt(tile_count)))
    strip_size = math.ceil(total_rows / strip_count)

    # Centroids are derived from the geometry bounding box (cheaper than ST_Centroid and stable
    # for every geometry type). file_row_number is used as a deterministic tie-breaker.
    return f"""
        WITH src AS (
            SELECT
                file_row_number,
                (ST_XMin(geometry) + ST_XMax(geometry)) / 2.0 AS __cx,
                (ST_YMin(geometry) + ST_YMax(geometry)) / 2.0 AS __cy
            FROM read_parquet('{input_file_path}', hive_partitioning=false, file_row_number=true)
        ),
        x_ranked AS (
            SELECT
                file_row_number,
                __cx,
                __cy,
                floor(
                    (row_number() OVER (ORDER BY __cx, __cy, file_row_number) - 1) / {strip_size}
                )::BIGINT AS __strip
            FROM src
        )
        SELECT
            file_row_number,
            row_number() OVER (
                ORDER BY
                    __strip,
                    CASE WHEN __strip % 2 = 0 THEN __cy ELSE -__cy END,
                    __cx,
                    file_row_number
            ) AS order_id
        FROM x_ranked
        """


def _sort_with_duckdb(
    input_file_path: Path,
    output_dir_path: Path,
    sort_extent: Optional[tuple[float, float, float, float]],
    tmp_dir_path: Path,
    verbosity_mode: "VERBOSITY_MODE",
    sort_algorithm: Literal["str", "hilbert"] = "str",
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    progress_callback: Optional["Callable[[int], None]"] = None,
    duckdb_conn_kwargs: Optional[DuckDBConnKwargs] = None,
) -> None:
    connection = set_up_duckdb_connection(
        tmp_dir_path,
        preserve_insertion_order=True,
        duckdb_conn_kwargs=duckdb_conn_kwargs,
    )

    if sort_algorithm == "str":
        index_select_sql = _str_index_select_sql(
            input_file_path=input_file_path,
            row_group_size=row_group_size,
            connection=connection,
        )
    elif sort_algorithm == "hilbert":
        index_select_sql = _hilbert_index_select_sql(
            input_file_path=input_file_path,
            sort_extent=sort_extent,
            connection=connection,
        )
    else:
        raise ValueError(f"Unknown sort algorithm: {sort_algorithm}")

    relation = connection.sql(index_select_sql)

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
                sql_query=sql_query,
                tmp_dir_path=tmp_dir_path,
                verbosity_mode=verbosity_mode,
                preserve_insertion_order=True,
                duckdb_conn_kwargs=duckdb_conn_kwargs,
            )

            current_file_idx += 1
            current_offset += current_limit
            if progress_callback:
                progress_callback(min(current_offset, total_rows))

        except (OutOfMemoryException, MemoryError) as ex:
            current_limit //= 10
            if current_limit == 1:
                raise

            if not verbosity_mode == "silent":
                rprint(
                    f"Encountered {ex.__class__.__name__} during operation."
                    f" Retrying with lower number of rows per batch ({current_limit} rows)."
                )
