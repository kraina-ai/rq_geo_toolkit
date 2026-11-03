"""Utilities for writing files safely."""

import secrets
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Union


@contextmanager
def atomic_output_path(output_file_path: Union[str, Path]) -> Generator[Path, None, None]:
    """
    Yield a temporary path that atomically replaces the output path on success.

    The temporary file is created next to the output file, so the final replace stays
    on the same filesystem. Concurrent writers of the same output never see a partial file
    and never compete for the same file lock - the last finished writer wins.

    Args:
        output_file_path (Union[str, Path]): Final file path.

    Yields:
        Path: Temporary file path to write to. Removed if the write fails.
    """
    output_file_path = Path(output_file_path)
    # fixed-length name, so long output file names don't exceed the filesystem limit
    tmp_file_path = output_file_path.with_name(f".{secrets.token_hex(8)}.tmp")
    try:
        yield tmp_file_path
        tmp_file_path.replace(output_file_path)
    finally:
        tmp_file_path.unlink(missing_ok=True)
