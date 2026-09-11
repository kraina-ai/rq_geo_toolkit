"""Cgroup-aware system memory detection."""

from __future__ import annotations

import logging
import re
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal, NamedTuple

import psutil

if TYPE_CHECKING:
    from collections.abc import Iterator

_LOGGER = logging.getLogger(__name__)


class MemoryStatus(NamedTuple):
    """Snapshot of system/container memory status."""

    total_bytes: int
    used_bytes: int
    available_bytes: int
    percent_used: float
    source: Literal["cgroup_v2", "cgroup_v1", "psutil"]


_DEFAULT_CGROUP_V2_MOUNT_POINT = Path("/sys/fs/cgroup")
_DEFAULT_CGROUP_V1_MOUNT_POINT = Path("/sys/fs/cgroup/memory")
_DEFAULT_PROC_SELF_CGROUP_PATH = Path("/proc/self/cgroup")
_DEFAULT_PROC_SELF_MOUNTINFO_PATH = Path("/proc/self/mountinfo")

_CGROUP_V2_CONTROLLERS_PATH = _DEFAULT_CGROUP_V2_MOUNT_POINT / "cgroup.controllers"
_CGROUP_V2_MEMORY_MAX_PATH = _DEFAULT_CGROUP_V2_MOUNT_POINT / "memory.max"
_CGROUP_V2_MEMORY_CURRENT_PATH = _DEFAULT_CGROUP_V2_MOUNT_POINT / "memory.current"
_CGROUP_V2_MEMORY_STAT_PATH = _DEFAULT_CGROUP_V2_MOUNT_POINT / "memory.stat"

_CGROUP_V1_MEMORY_PATH = _DEFAULT_CGROUP_V1_MOUNT_POINT
_CGROUP_V1_LIMIT_PATH = _CGROUP_V1_MEMORY_PATH / "memory.limit_in_bytes"
_CGROUP_V1_USAGE_PATH = _CGROUP_V1_MEMORY_PATH / "memory.usage_in_bytes"
_CGROUP_V1_STAT_PATH = _CGROUP_V1_MEMORY_PATH / "memory.stat"

_PROC_SELF_CGROUP_PATH = _DEFAULT_PROC_SELF_CGROUP_PATH
_PROC_SELF_MOUNTINFO_PATH = _DEFAULT_PROC_SELF_MOUNTINFO_PATH

_CGROUP_UNLIMITED_SENTINEL_V1 = 9_223_372_036_854_771_712


def _read_text_from_file(path: Path) -> str | None:
    """Read text from a file, returning None on failure."""
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return None


def _read_int_from_file(path: Path) -> int | None:
    """Read an integer from a file, returning None on failure."""
    text = _read_text_from_file(path)
    if text is None:
        return None
    raw = text.strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _parse_memory_stat_inactive_file_v2(stat_text: str) -> int:
    """Parse inactive_file value from cgroup v2 memory.stat."""
    for line in stat_text.splitlines():
        if line.startswith("inactive_file "):
            _, value = line.split(" ", 1)
            try:
                return int(value)
            except ValueError:
                return 0
    return 0


def _parse_memory_stat_inactive_file_v1(stat_text: str) -> int:
    """Parse total_inactive_file value from cgroup v1 memory.stat."""
    for line in stat_text.splitlines():
        if line.startswith("total_inactive_file "):
            _, value = line.split(" ", 1)
            try:
                return int(value)
            except ValueError:
                return 0
    return 0


class _CgroupMount(NamedTuple):
    mount_point: Path
    mount_root: PurePosixPath
    filesystem: str
    controllers: frozenset[str]


def _decode_mountinfo_path(value: str) -> str:
    return re.sub(
        r"\\([0-7]{3})",
        lambda match: chr(int(match.group(1), 8)),
        value,
    )


def _parse_cgroup_mounts() -> list[_CgroupMount]:
    text = _read_text_from_file(_PROC_SELF_MOUNTINFO_PATH)
    if text is None:
        return []

    mounts: list[_CgroupMount] = []
    for line in text.splitlines():
        fields = line.split()
        try:
            separator = fields.index("-")
        except ValueError:
            continue
        if len(fields) <= separator + 3 or len(fields) <= 5:
            continue

        filesystem = fields[separator + 1]
        mount_point = Path(_decode_mountinfo_path(fields[4])).resolve()
        mount_root = PurePosixPath(_decode_mountinfo_path(fields[3]))
        controllers = frozenset(
            {
                *fields[5].split(","),
                *fields[separator + 3].split(","),
            }
        )
        mounts.append(
            _CgroupMount(
                mount_point,
                mount_root,
                filesystem,
                controllers,
            )
        )
    return mounts


def _get_process_cgroup_path(controller: str | None) -> PurePosixPath | None:
    text = _read_text_from_file(_PROC_SELF_CGROUP_PATH)
    if text is None:
        return PurePosixPath("/")

    for line in text.splitlines():
        fields = line.split(":", 2)
        if len(fields) != 3:
            continue
        _, controllers, path = fields
        if controller is None:
            if controllers == "":
                candidate = PurePosixPath(path)
                if candidate.is_absolute():
                    return candidate
        elif controller in controllers.split(","):
            candidate = PurePosixPath(path)
            if candidate.is_absolute():
                return candidate
    return None


def _get_cgroup_mounts(
    filesystem: str,
    controller: str | None,
    configured_mount_point: Path,
    default_mount_point: Path,
) -> list[_CgroupMount]:
    mounts = [
        mount
        for mount in _parse_cgroup_mounts()
        if mount.filesystem == filesystem
        and (controller is None or controller in mount.controllers)
    ]
    mountinfo_is_configured = _PROC_SELF_MOUNTINFO_PATH != _DEFAULT_PROC_SELF_MOUNTINFO_PATH
    if mountinfo_is_configured:
        return mounts or [
            _CgroupMount(
                configured_mount_point.resolve(),
                PurePosixPath("/"),
                filesystem,
                frozenset({controller} if controller is not None else set()),
            )
        ]
    if configured_mount_point != default_mount_point:
        matching_mounts = [
            mount for mount in mounts if mount.mount_point == configured_mount_point.resolve()
        ]
        if matching_mounts:
            return matching_mounts
        return [
            _CgroupMount(
                configured_mount_point.resolve(),
                PurePosixPath("/"),
                filesystem,
                frozenset({controller} if controller is not None else set()),
            )
        ]
    return mounts or [
        _CgroupMount(
            configured_mount_point.resolve(),
            PurePosixPath("/"),
            filesystem,
            frozenset({controller} if controller is not None else set()),
        )
    ]


def _relative_cgroup_path(
    process_path: PurePosixPath,
    mount_root: PurePosixPath,
) -> PurePosixPath | None:
    process = str(process_path)
    root = str(mount_root)

    if root == "/":
        if process == "/":
            return PurePosixPath(".")
        if not process.startswith("/"):
            return None
        relative = process
    else:
        if process == root:
            return PurePosixPath(".")
        prefix = root + "/"
        if not process.startswith(prefix):
            return None
        relative = process[len(root) :]

    if relative == "/.." or relative.startswith("/../"):
        return None
    return PurePosixPath(relative.lstrip("/"))


def _resolve_cgroup_directory(
    process_path: PurePosixPath,
    mount: _CgroupMount,
) -> Path | None:
    relative_path = _relative_cgroup_path(process_path, mount.mount_root)
    if relative_path is None:
        return None

    directory = (
        mount.mount_point
        if relative_path == PurePosixPath(".")
        else mount.mount_point.joinpath(*relative_path.parts)
    ).resolve()
    try:
        directory.relative_to(mount.mount_point)
    except ValueError:
        return None
    return directory


def _iter_cgroup_directories(
    directory: Path,
    mount_point: Path,
) -> Iterator[Path]:
    while True:
        yield directory
        if directory == mount_point:
            return
        try:
            directory.relative_to(mount_point)
        except ValueError:
            return
        parent = directory.parent
        if parent == directory:
            return
        directory = parent


def _find_cgroup_v2_memory_paths() -> tuple[Path, Path] | None:
    process_path = _get_process_cgroup_path(None)
    if process_path is None:
        return None
    configured_mount_point = _CGROUP_V2_CONTROLLERS_PATH.parent
    for mount in _get_cgroup_mounts(
        "cgroup2",
        None,
        configured_mount_point,
        _DEFAULT_CGROUP_V2_MOUNT_POINT,
    ):
        directory = _resolve_cgroup_directory(process_path, mount)
        if directory is None:
            continue

        best: tuple[int, Path, Path] | None = None
        for cgroup_directory in _iter_cgroup_directories(
            directory,
            mount.mount_point,
        ):
            limit_path = cgroup_directory / "memory.max"
            usage_path = cgroup_directory / "memory.current"
            limit = _read_int_from_file(limit_path)
            usage = _read_int_from_file(usage_path)
            if limit is None or limit <= 0 or usage is None:
                continue

            if best is None or limit < best[0]:
                best = (limit, limit_path, usage_path)
        if best is not None:
            return best[1], best[2]
    return None


def _find_cgroup_v1_memory_paths() -> tuple[Path, Path] | None:
    process_path = _get_process_cgroup_path("memory")
    if process_path is None:
        return None
    configured_mount_point = _CGROUP_V1_MEMORY_PATH
    for mount in _get_cgroup_mounts(
        "cgroup",
        "memory",
        configured_mount_point,
        _DEFAULT_CGROUP_V1_MOUNT_POINT,
    ):
        directory = _resolve_cgroup_directory(process_path, mount)
        if directory is None:
            continue

        best: tuple[int, Path, Path] | None = None
        for cgroup_directory in _iter_cgroup_directories(
            directory,
            mount.mount_point,
        ):
            limit_path = cgroup_directory / "memory.limit_in_bytes"
            usage_path = cgroup_directory / "memory.usage_in_bytes"
            limit = _read_int_from_file(limit_path)
            usage = _read_int_from_file(usage_path)
            if (
                limit is None
                or limit <= 0
                or limit >= _CGROUP_UNLIMITED_SENTINEL_V1
                or usage is None
            ):
                continue

            if best is None or limit < best[0]:
                best = (limit, limit_path, usage_path)
        if best is not None:
            return best[1], best[2]
    return None


def _get_cgroup_v2_memory_status(
    limit_path: Path | None = None,
    usage_path: Path | None = None,
) -> MemoryStatus | None:
    """Attempt to read memory status from cgroup v2."""
    limit_path = limit_path or _CGROUP_V2_MEMORY_MAX_PATH
    usage_path = usage_path or limit_path.with_name("memory.current")
    stat_path = usage_path.with_name("memory.stat")
    limit = _read_int_from_file(limit_path)
    if limit is None or limit <= 0:
        return None

    raw_usage = _read_int_from_file(usage_path)
    if raw_usage is None:
        return None

    inactive_file = 0
    stat_content = _read_text_from_file(stat_path)
    if stat_content is not None:
        inactive_file = _parse_memory_stat_inactive_file_v2(stat_content)

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


def _get_cgroup_v1_memory_status(
    limit_path: Path | None = None,
    usage_path: Path | None = None,
) -> MemoryStatus | None:
    """Attempt to read memory status from cgroup v1."""
    limit_path = limit_path or _CGROUP_V1_LIMIT_PATH
    usage_path = usage_path or _CGROUP_V1_USAGE_PATH
    stat_path = usage_path.with_name("memory.stat")
    limit = _read_int_from_file(limit_path)
    if limit is None or limit <= 0:
        return None

    if limit >= _CGROUP_UNLIMITED_SENTINEL_V1:
        return None

    raw_usage = _read_int_from_file(usage_path)
    if raw_usage is None:
        return None

    inactive_file = 0
    stat_content = _read_text_from_file(stat_path)
    if stat_content is not None:
        inactive_file = _parse_memory_stat_inactive_file_v1(stat_content)

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
    v2_paths = _find_cgroup_v2_memory_paths()
    if v2_paths is not None:
        return "cgroup_v2", v2_paths[0], v2_paths[1]

    v1_paths = _find_cgroup_v1_memory_paths()
    if v1_paths is not None:
        return "cgroup_v1", v1_paths[0], v1_paths[1]

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
    source, limit_path, usage_path = _detect_memory_source()

    if source == "cgroup_v2":
        status = _get_cgroup_v2_memory_status(limit_path, usage_path)
    elif source == "cgroup_v1":
        status = _get_cgroup_v1_memory_status(limit_path, usage_path)
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
