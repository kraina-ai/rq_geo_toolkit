"""Test atomic file writing."""

import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from rq_geo_toolkit.file_utils import atomic_output_path


def test_concurrent_writers_produce_complete_file() -> None:
    """Test if concurrent writers of the same output always leave a complete file."""
    table = pa.table({"value": list(range(100_000))})

    with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp_dir_name:
        output_file_path = Path(tmp_dir_name) / "result.parquet"

        def write(_: int) -> None:
            with atomic_output_path(output_file_path) as tmp_file_path:
                pq.write_table(table, tmp_file_path)

        writing_finished = threading.Event()
        complete_reads = 0

        def read() -> None:
            nonlocal complete_reads
            while not writing_finished.is_set():
                try:
                    read_table = pq.read_table(output_file_path)
                except FileNotFoundError:
                    continue
                assert read_table.equals(table)
                complete_reads += 1

        with ThreadPoolExecutor(max_workers=9) as executor:
            reader = executor.submit(read)
            list(executor.map(write, range(32)))
            writing_finished.set()
            reader.result()

        assert complete_reads > 0
        assert pq.read_table(output_file_path).equals(table)
        assert list(Path(tmp_dir_name).iterdir()) == [output_file_path]


def test_failed_write_leaves_no_files() -> None:
    """Test if a failed write removes the temporary file and keeps the old output."""
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp_dir_name:
        output_file_path = Path(tmp_dir_name) / "result.txt"
        output_file_path.write_text("old")

        with pytest.raises(ValueError), atomic_output_path(output_file_path) as tmp_file_path:
            tmp_file_path.write_text("partial")
            raise ValueError

        assert output_file_path.read_text() == "old"
        assert list(Path(tmp_dir_name).iterdir()) == [output_file_path]


def test_long_output_file_name() -> None:
    """Test if output file names close to the filesystem limit can be written."""
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp_dir_name:
        output_file_path = Path(tmp_dir_name) / f"{'a' * 247}.parquet"

        with atomic_output_path(output_file_path) as tmp_file_path:
            tmp_file_path.write_text("content")

        assert output_file_path.read_text() == "content"
