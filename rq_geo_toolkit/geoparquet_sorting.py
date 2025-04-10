"""Module for sorting GeoParquet files."""

import multiprocessing
import tempfile
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from rq_geo_toolkit.constants import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    PARQUET_ROW_GROUP_SIZE,
)
from rq_geo_toolkit.duckdb import set_up_duckdb_connection
from rq_geo_toolkit.geoparquet_compression import (
    compress_parquet_with_duckdb,
)

if TYPE_CHECKING:  # pragma: no cover
    from rq_geo_toolkit.rich_utils import VERBOSITY_MODE

MEMORY_1GB = 1024**3


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

        _sort_with_multiprocessing(
            input_file_path=input_file_path,
            output_dir_path=order_dir_path,
            sort_extent=sort_extent,
            tmp_dir_path=tmp_dir_path,
            row_group_size=row_group_size,
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
            verbosity_mode=verbosity_mode
        )

    return output_file_path


def _sort_with_multiprocessing(
    input_file_path: Path,
    output_dir_path: Path,
    sort_extent: Optional[tuple[float, float, float, float]],
    row_group_size: int,
    tmp_dir_path: Path,
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

    order_file_path = tmp_dir_path / "order_index.parquet"

    relation.to_parquet(
        str(order_file_path),
        row_group_size=row_group_size,
    )

    connection.close()

    # Calculate mapping of ranges which file row ids exist in each row group
    original_file_row_group_mapping = _calculate_row_group_mapping(input_file_path)

    # Order each row group from the ordered index in separate processes by reading
    # selected row groups from the original file
    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn")) as ex:
        fn = partial(
            _order_single_row_group,
            output_dir_path=output_dir_path,
            order_file_path=order_file_path,
            original_file_path=input_file_path,
            original_file_row_group_mapping=original_file_row_group_mapping,
        )
        ex.map(
            fn,
            list(range(pq.read_metadata(order_file_path).num_row_groups)),
            chunksize=1,
        )


def _order_single_row_group(
    row_group_id: int,
    output_dir_path: Path,
    order_file_path: Path,
    original_file_path: Path,
    original_file_row_group_mapping: dict[int, tuple[int, int]],
) -> None:
    # Calculate row_groups and local indexes withing those row_groups
    ordering_row_group_extended = (
        pl.from_arrow(pq.ParquetFile(order_file_path).read_row_group(row_group_id))
        .with_columns(
            # Assign row group based on file row number using original file mapping
            pl.col("file_row_number")
            .map_elements(
                lambda row_number: next(
                    row_group_id
                    for row_group_id, (
                        start_row_number,
                        end_row_number,
                    ) in original_file_row_group_mapping.items()
                    if start_row_number <= row_number <= end_row_number
                ),
                return_dtype=pl.Int64(),
            )
            .alias("row_group_id")
        )
        .with_columns(
            # Assign local row index within the row group using
            # original file mapping and total row number
            pl.struct(pl.col("file_row_number"), pl.col("row_group_id"))
            .map_elements(
                lambda struct: struct["file_row_number"]
                - original_file_row_group_mapping[struct["row_group_id"]][0],
                return_dtype=pl.Int64(),
            )
            .alias("local_index")
        )
    )

    # Example of ordered file mapping:
    # order_id, file_row_number, row_group_id, local_index
    # 1,        1,               1,            0
    # 2,        5,               1,            5
    # 3,        15,              2,            1
    # 4,        3,               1,            3

    # Read all expected rows from each row group at once to avoid multiple reads
    # Group matching consecutive row group and save local indexes
    # indexes_to_read_per_row_group = {1: [0, 5, 3], 2: [1]}
    # reshuffled_indexes_to_read = [(1, [0, 5]), (2, [1]), (1, [3])]

    # Dictionary with row group id and a list of local indices to read from each row group.
    indexes_to_read_per_row_group: dict[int, list[int]] = {}
    # Grouped list of local indexes to read per row group in order
    reshuffled_indexes_to_read: list[tuple[int, list[int]]] = []
    # Cache objects to keep track of each group withing multiple row groups
    current_index_per_row_group: dict[int, int] = {}
    current_reshuffled_indexes_group: list[int] = []
    current_rg_id = ordering_row_group_extended["row_group_id"][0]

    # Iterate rows in order
    for rg_id, local_index in ordering_row_group_extended[
        ["row_group_id", "local_index"]
    ].iter_rows():
        if rg_id not in indexes_to_read_per_row_group:
            indexes_to_read_per_row_group[rg_id] = []
            current_index_per_row_group[rg_id] = 0

        indexes_to_read_per_row_group[rg_id].append(local_index)

        if rg_id != current_rg_id:
            reshuffled_indexes_to_read.append((current_rg_id, current_reshuffled_indexes_group))
            current_rg_id = rg_id
            current_reshuffled_indexes_group = [current_index_per_row_group[rg_id]]
        else:
            current_reshuffled_indexes_group.append(current_index_per_row_group[rg_id])

        current_index_per_row_group[rg_id] += 1

    if current_reshuffled_indexes_group:
        reshuffled_indexes_to_read.append((current_rg_id, current_reshuffled_indexes_group))

    # Read expected rows per row group
    read_tables_per_row_group = {
        rg_id: pq.ParquetFile(original_file_path).read_row_group(rg_id).take(local_rows_ids)
        for rg_id, local_rows_ids in indexes_to_read_per_row_group.items()
    }

    # Read rows from each read row group using reshuffled local indexes
    concatenated_tables = pa.concat_tables(
        [
            read_tables_per_row_group[rg_id].take(reshuffled_indexes)
            for rg_id, reshuffled_indexes in reshuffled_indexes_to_read
        ]
    )
    pq.write_table(table=concatenated_tables, where=output_dir_path / f"{row_group_id}.parquet")


def _calculate_row_group_mapping(file_path: Path) -> dict[int, tuple[int, int]]:
    pq_f = pq.ParquetFile(file_path)

    mapping = {}
    total_rows = 0
    for i in range(pq_f.num_row_groups):
        start_index = total_rows
        rows_in_row_group = pq_f.metadata.row_group(i).num_rows
        total_rows += rows_in_row_group
        end_index = total_rows - 1
        mapping[i] = (start_index, end_index)

    return mapping
