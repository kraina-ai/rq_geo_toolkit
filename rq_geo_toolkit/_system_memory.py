"""Cgroup-aware system memory detection."""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Literal, NamedTuple

import psutil

_LOGGER = logging.getLogger(__name__)


class MemoryStatus(NamedTuple):
    """Snapshot of system/container memory status."""

    total_bytes: int
    used_bytes: int
    available_bytes: int
    percent_used: float
    source: Literal["cgroup_v2", "cgroup_v1", "psutil"]


_CGROUP_V2_CONTROLLERS_PATH = Path("/sys/fs/cgroup/cgroup.controllers")
_CGROUP_V2_MEMORY_MAX_PATH = Path("/sys/fs/cgroup/memory.max")
_CGROUP_V2_MEMORY_CURRENT_PATH = Path("/sys/fs/cgroup/memory.current")
_CGROUP_V2_MEMORY_STAT_PATH = Path("/sys/fs/cgroup/memory.stat")

_CGROUP_V1_MEMORY_PATH = Path("/sys/fs/cgroup/memory")
_CGROUP_V1_LIMIT_PATH = _CGROUP_V1_MEMORY_PATH / "memory.limit_in_bytes"
_CGROUP_V1_USAGE_PATH = _CGROUP_V1_MEMORY_PATH / "memory.usage_in_bytes"
_CGROUP_V1_STAT_PATH = _CGROUP_V1_MEMORY_PATH / "memory.stat"

_CGROUP_UNLIMITED_SENTINEL_V1 = 9_223_372_036_854_771_712


def _read_int_from_file(path: Path) -> int | None:
    """Read an integer from a file, returning None on failure."""
    try:
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        return int(raw)
    except OSError:
        return None
    except ValueError:
        return None


def _parse_memory_stat_inactive_file_v2(stat_text: str) -> int:
    """Parse inactive_file value from cgroup v2 memory.stat."""
    for line in stat_text.splitlines():
        if line.startswith("inactive_file "):
            _, value = line.split(" ", 1)
            return int(value)
    return 0


def _parse_memory_stat_inactive_file_v1(stat_text: str) -> int:
    """Parse total_inactive_file value from cgroup v1 memory.stat."""
    for line in stat_text.splitlines():
        if line.startswith("total_inactive_file "):
            _, value = line.split(" ", 1)
            return int(value)
    return 0


def _get_cgroup_v2_memory_status() -> MemoryStatus | None:
    """Attempt to read memory status from cgroup v2."""
    limit = _read_int_from_file(_CGROUP_V2_MEMORY_MAX_PATH)
    if limit is None:
        return None

    if _CGROUP_V2_MEMORY_MAX_PATH.read_text(encoding="utf-8").strip() == "max":
        return None

    raw_usage = _read_int_from_file(_CGROUP_V2_MEMORY_CURRENT_PATH)
    if raw_usage is None:
        return None

    inactive_file = 0
    if _CGROUP_V2_MEMORY_STAT_PATH.is_file():
        try:
            stat_content = _CGROUP_V2_MEMORY_STAT_PATH.read_text(encoding="utf-8")
            inactive_file = _parse_memory_stat_inactive_file_v2(stat_content)
        except OSError:
            pass

    used_bytes = max(raw_usage - inactive_file, 0)
    available_bytes = max(limit - used_bytes, 0)
    percent_used = used_bytes / limit * 100 if limit > 0 else 0.0

    return MemoryStatus(
        total_bytes=limit,
        used_bytes=used_bytes,
        available_bytes=available_bytes,
        percent_used=percent_used,
        source="cgroup_v2",
    )


def _get_cgroup_v1_memory_status() -> MemoryStatus | None:
    """Attempt to read memory status from cgroup v1."""
    if not _CGROUP_V1_MEMORY_PATH.is_dir():
        return None

    limit = _read_int_from_file(_CGROUP_V1_LIMIT_PATH)
    if limit is None:
        return None

    if limit >= _CGROUP_UNLIMITED_SENTINEL_V1:
        host_total = psutil.virtual_memory().total
        if limit >= host_total:
            return None

    raw_usage = _read_int_from_file(_CGROUP_V1_USAGE_PATH)
    if raw_usage is None:
        return None

    inactive_file = 0
    if _CGROUP_V1_STAT_PATH.is_file():
        try:
            stat_content = _CGROUP_V1_STAT_PATH.read_text(encoding="utf-8")
            inactive_file = _parse_memory_stat_inactive_file_v1(stat_content)
        except OSError:
            pass

    used_bytes = max(raw_usage - inactive_file, 0)
    available_bytes = max(limit - used_bytes, 0)
    percent_used = used_bytes / limit * 100 if limit > 0 else 0.0

    return MemoryStatus(
        total_bytes=limit,
        used_bytes=used_bytes,
        available_bytes=available_bytes,
        percent_used=percent_used,
        source="cgroup_v1",
    )


def _get_psutil_memory_status() -> MemoryStatus:
    """Read memory status from psutil."""
    mem = psutil.virtual_memory()
    return MemoryStatus(
        total_bytes=mem.total,
        used_bytes=mem.used,
        available_bytes=mem.available,
        percent_used=mem.percent,
        source="psutil",
    )


@lru_cache(maxsize=1)
def _detect_memory_source() -> tuple[
    Literal["cgroup_v2", "cgroup_v1", "psutil"],
    Path | None,
    Path | None,
]:
    """Detect which memory source to use and cache the decision."""
    if _CGROUP_V2_CONTROLLERS_PATH.is_file():
        v2_limit_path = _CGROUP_V2_MEMORY_MAX_PATH
        v2_limit_text = ""
        try:
            v2_limit_text = v2_limit_path.read_text(encoding="utf-8").strip()
        except OSError:
            pass

        if v2_limit_text and v2_limit_text != "max":
            try:
                limit = int(v2_limit_text)
                if limit > 0:
                    return "cgroup_v2", v2_limit_path, _CGROUP_V2_MEMORY_CURRENT_PATH
            except ValueError:
                pass

    if _CGROUP_V1_MEMORY_PATH.is_dir():
        v1_limit_path = _CGROUP_V1_LIMIT_PATH
        v1_limit_text = ""
        try:
            v1_limit_text = v1_limit_path.read_text(encoding="utf-8").strip()
        except OSError:
            pass

        if v1_limit_text:
            try:
                limit = int(v1_limit_text)
                if 0 < limit < _CGROUP_UNLIMITED_SENTINEL_V1:
                    return "cgroup_v1", v1_limit_path, _CGROUP_V1_USAGE_PATH
                host_total = psutil.virtual_memory().total
                if 0 < limit < host_total:
                    return "cgroup_v1", v1_limit_path, _CGROUP_V1_USAGE_PATH
            except (ValueError, OSError):
                pass

    return "psutil", None, None


def get_memory_status(total_bytes_override: int | None = None) -> MemoryStatus:
    """
    Return current memory status, preferring cgroup-scoped values when available.

    Args:
        total_bytes_override: When provided, use this value as the total memory limit
            instead of auto-detecting it from cgroup/psutil. Usage and percent are still
            read from the best available source.

    Returns:
        MemoryStatus with total, used, available, percent, and the source used.
    """
    source, _, _ = _detect_memory_source()

    if source == "cgroup_v2":
        status = _get_cgroup_v2_memory_status()
    elif source == "cgroup_v1":
        status = _get_cgroup_v1_memory_status()
    else:
        status = None

    if status is None:
        status = _get_psutil_memory_status()

    _LOGGER.debug(
        "Memory status resolved: source=%s total_bytes=%d",
        status.source,
        status.total_bytes,
    )

    if total_bytes_override is not None:
        used = status.used_bytes
        available = max(total_bytes_override - used, 0)
        percent = used / total_bytes_override * 100 if total_bytes_override > 0 else 0.0
        status = MemoryStatus(
            total_bytes=total_bytes_override,
            used_bytes=used,
            available_bytes=available,
            percent_used=percent,
            source=status.source,
        )

    return status
