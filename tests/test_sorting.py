"""Tests for sorting and compressing geoparquet files."""

import tempfile
from pathlib import Path
from typing import Literal

import pyarrow.parquet as pq
import pytest
from tqdm import tqdm

from rq_geo_toolkit.duckdb import set_up_duckdb_connection
from rq_geo_toolkit.geoparquet_compression import compress_parquet_with_duckdb
from rq_geo_toolkit.geoparquet_sorting import sort_geoparquet_file_by_geometry
from tests.conftest import load_biggest_overture_place_file_from_stac


@pytest.fixture(scope="session")  # type: ignore
def unsorted_example_file() -> Path:
    """
    Prepare a randomly-ordered Overture place file used by the sorting tests.

    The file is downloaded and shuffled only once per session and cached on disk, so repeated test
    runs (and the parametrized algorithms) reuse it instead of fetching the source from S3 every
    time.
    """
    save_path = Path("files/unsorted_example.parquet")
    save_path.parent.mkdir(exist_ok=True, parents=True)

    if not save_path.exists():
        download_file_url = load_biggest_overture_place_file_from_stac()
        query = f"""
        COPY (
            SELECT id, geometry
            FROM read_parquet('{download_file_url}')
            ORDER BY random()
        ) TO '{save_path}' (FORMAT parquet);
        """
        with tempfile.TemporaryDirectory(dir=save_path.parent.resolve()) as tmp_dir_name:
            with set_up_duckdb_connection(
                tmp_dir_path=Path(tmp_dir_name), preserve_insertion_order=True
            ) as connection:
                connection.execute(query)

    return save_path


@pytest.mark.parametrize("sort_algorithm", ["str", "hilbert"])  # type: ignore
def test_sorting(sort_algorithm: Literal["str", "hilbert"], unsorted_example_file: Path) -> None:
    """Test that each sorting algorithm shrinks the file and keeps metadata equal."""
    save_path = unsorted_example_file

    with tempfile.TemporaryDirectory(dir=save_path.parent.resolve()) as tmp_dir_name:
        tmp_dir_path = Path(tmp_dir_name)

        unsorted_pq = compress_parquet_with_duckdb(
            input_file_path=save_path,
            output_file_path=tmp_dir_path / "unsorted.parquet",
            working_directory=tmp_dir_path,
        )

        total_rows = pq.read_metadata(save_path).num_rows
        last_rows = 0

        with tqdm(total=total_rows, desc=sort_algorithm) as pbar:

            def report_progress(n: int) -> None:
                nonlocal last_rows, pbar
                diff = n - last_rows
                pbar.update(diff)
                last_rows = n

            sorted_pq = sort_geoparquet_file_by_geometry(
                input_file_path=save_path,
                output_file_path=tmp_dir_path / "sorted.parquet",
                sort_algorithm=sort_algorithm,
                working_directory=tmp_dir_path,
                remove_input_file=False,
                progress_callback=report_progress,
            )

        assert pq.read_schema(unsorted_pq).equals(pq.read_schema(sorted_pq))
        assert pq.read_metadata(unsorted_pq).num_rows == pq.read_metadata(sorted_pq).num_rows

        # Spatial sorting clusters nearby geometries, which compresses better than the
        # original random order - so each algorithm must produce a smaller file.
        assert unsorted_pq.stat().st_size > sorted_pq.stat().st_size
