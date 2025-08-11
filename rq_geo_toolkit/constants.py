"""Constant values used inside the library and for external use."""

from typing import Literal

GEOMETRY_COLUMN = "geometry"

PARQUET_ROW_GROUP_SIZE = 100_000
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3
PARQUET_VERSION: Literal["v2"] = "v2"

MEMORY_1GB = 1024**3
