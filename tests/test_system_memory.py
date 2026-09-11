"""Tests for cgroup-aware memory detection."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

import psutil
import pytest

from rq_geo_toolkit._system_memory import (
    _detect_memory_source,
    _get_psutil_memory_status,
    get_memory_status,
)

if TYPE_CHECKING:
    from pathlib import Path


def _clear_caches() -> None:
    """Clear lru_cache between tests."""
    _detect_memory_source.cache_clear()


def test_cgroup_v2_with_limit_and_inactive_file(tmp_path: Path) -> None:
    """Cgroup v2 with real limit and inactive_file gives cache-adjusted usage."""
    _clear_caches()
    v2_base = tmp_path / "cgroup2"
    v2_base.mkdir()

    (v2_base / "cgroup.controllers").write_text("")
    (v2_base / "memory.max").write_text("8589934592")
    (v2_base / "memory.current").write_text("4294967296")
    (v2_base / "memory.stat").write_text("inactive_file 1073741824\n")

    with patch.multiple(
        "rq_geo_toolkit._system_memory",
        _CGROUP_V2_CONTROLLERS_PATH=v2_base / "cgroup.controllers",
        _CGROUP_V2_MEMORY_MAX_PATH=v2_base / "memory.max",
        _CGROUP_V2_MEMORY_CURRENT_PATH=v2_base / "memory.current",
        _CGROUP_V2_MEMORY_STAT_PATH=v2_base / "memory.stat",
    ):
        status = get_memory_status()
    assert status.source == "cgroup_v2"
    assert status.total_bytes == 8589934592
    assert status.used_bytes == 3221225472  # 4294967296 - 1073741824
    assert status.available_bytes == 5368709120
    assert pytest.approx(status.percent_used) == 100 * 3221225472 / 8589934592


def test_cgroup_v2_max_unlimited_falls_through(tmp_path: Path) -> None:
    """Cgroup v2 with memory.max == 'max' falls through to psutil."""
    _clear_caches()
    v2_base = tmp_path / "cgroup2"
    v2_base.mkdir()

    (v2_base / "cgroup.controllers").write_text("")
    (v2_base / "memory.max").write_text("max")

    with patch.multiple(
        "rq_geo_toolkit._system_memory",
        _CGROUP_V2_CONTROLLERS_PATH=v2_base / "cgroup.controllers",
        _CGROUP_V2_MEMORY_MAX_PATH=v2_base / "memory.max",
    ):
        status = get_memory_status()
    assert status.source == "psutil"
    assert status.total_bytes == psutil.virtual_memory().total


def test_cgroup_v2_missing_stat_uses_raw_usage(tmp_path: Path) -> None:
    """Cgroup v2 without memory.stat falls back to raw usage."""
    _clear_caches()
    v2_base = tmp_path / "cgroup2"
    v2_base.mkdir()

    (v2_base / "cgroup.controllers").write_text("")
    (v2_base / "memory.max").write_text("8589934592")
    (v2_base / "memory.current").write_text("4294967296")

    with patch.multiple(
        "rq_geo_toolkit._system_memory",
        _CGROUP_V2_CONTROLLERS_PATH=v2_base / "cgroup.controllers",
        _CGROUP_V2_MEMORY_MAX_PATH=v2_base / "memory.max",
        _CGROUP_V2_MEMORY_CURRENT_PATH=v2_base / "memory.current",
        _CGROUP_V2_MEMORY_STAT_PATH=v2_base / "nonexistent",
    ):
        status = get_memory_status()
    assert status.source == "cgroup_v2"
    assert status.used_bytes == 4294967296


def test_cgroup_v1_with_limit_and_inactive_file(tmp_path: Path) -> None:
    """Cgroup v1 with real limit and total_inactive_file gives cache-adjusted usage."""
    _clear_caches()
    v1_base = tmp_path / "memory"
    v1_base.mkdir()

    (v1_base / "memory.limit_in_bytes").write_text("8589934592")
    (v1_base / "memory.usage_in_bytes").write_text("4294967296")
    (v1_base / "memory.stat").write_text("total_inactive_file 1073741824\n")

    with patch.multiple(
        "rq_geo_toolkit._system_memory",
        _CGROUP_V2_CONTROLLERS_PATH=tmp_path / "nonexistent",
        _CGROUP_V1_MEMORY_PATH=v1_base,
        _CGROUP_V1_LIMIT_PATH=v1_base / "memory.limit_in_bytes",
        _CGROUP_V1_USAGE_PATH=v1_base / "memory.usage_in_bytes",
        _CGROUP_V1_STAT_PATH=v1_base / "memory.stat",
    ):
        status = get_memory_status()
    assert status.source == "cgroup_v1"
    assert status.total_bytes == 8589934592
    assert status.used_bytes == 3221225472
    assert pytest.approx(status.percent_used) == 100 * 3221225472 / 8589934592


def test_cgroup_v1_unlimited_sentinel_falls_through(tmp_path: Path) -> None:
    """Cgroup v1 with unlimited sentinel falls through to psutil."""
    _clear_caches()
    v1_base = tmp_path / "memory"
    v1_base.mkdir()

    (v1_base / "memory.limit_in_bytes").write_text("9223372036854771712")
    (v1_base / "memory.usage_in_bytes").write_text("12345")

    with patch.multiple(
        "rq_geo_toolkit._system_memory",
        _CGROUP_V2_CONTROLLERS_PATH=tmp_path / "nonexistent",
        _CGROUP_V1_MEMORY_PATH=v1_base,
        _CGROUP_V1_LIMIT_PATH=v1_base / "memory.limit_in_bytes",
        _CGROUP_V1_USAGE_PATH=v1_base / "memory.usage_in_bytes",
    ):
        status = get_memory_status()
    assert status.source == "psutil"


def test_no_cgroup_files_uses_psutil(tmp_path: Path) -> None:
    """Absence of cgroup files falls back to psutil."""
    _clear_caches()
    with patch.multiple(
        "rq_geo_toolkit._system_memory",
        _CGROUP_V2_CONTROLLERS_PATH=tmp_path / "nonexistent",
        _CGROUP_V1_MEMORY_PATH=tmp_path / "nonexistent",
    ):
        status = get_memory_status()
    assert status.source == "psutil"
    assert status.total_bytes == psutil.virtual_memory().total


def test_cgroup_files_unreadable_falls_through(tmp_path: Path) -> None:
    """Unreadable/malformed cgroup files fall through to psutil without exception."""
    _clear_caches()
    v2_base = tmp_path / "cgroup2"
    v2_base.mkdir()
    controllers = v2_base / "cgroup.controllers"
    controllers.write_text("")

    max_path = v2_base / "memory.max"
    max_path.write_text("not-a-number")

    with patch(
        "rq_geo_toolkit._system_memory._CGROUP_V2_CONTROLLERS_PATH", controllers
    ), patch("rq_geo_toolkit._system_memory._CGROUP_V2_MEMORY_MAX_PATH", max_path):
        status = get_memory_status()
    assert status.source == "psutil"


def test_used_bytes_clamped_to_zero(tmp_path: Path) -> None:
    """used_bytes is clamped to 0 when inactive_file exceeds raw usage."""
    _clear_caches()
    v2_base = tmp_path / "cgroup2"
    v2_base.mkdir()

    (v2_base / "cgroup.controllers").write_text("")
    (v2_base / "memory.max").write_text("8589934592")
    (v2_base / "memory.current").write_text("100")
    (v2_base / "memory.stat").write_text("inactive_file 200\n")

    with patch.multiple(
        "rq_geo_toolkit._system_memory",
        _CGROUP_V2_CONTROLLERS_PATH=v2_base / "cgroup.controllers",
        _CGROUP_V2_MEMORY_MAX_PATH=v2_base / "memory.max",
        _CGROUP_V2_MEMORY_CURRENT_PATH=v2_base / "memory.current",
        _CGROUP_V2_MEMORY_STAT_PATH=v2_base / "memory.stat",
    ):
        status = get_memory_status()
    assert status.used_bytes == 0
    assert status.available_bytes == status.total_bytes


def test_manual_override_total_bytes(tmp_path: Path) -> None:
    """total_bytes_override sets total and recomputes percent correctly."""
    _clear_caches()
    v2_base = tmp_path / "cgroup2"
    v2_base.mkdir()

    (v2_base / "cgroup.controllers").write_text("")
    (v2_base / "memory.max").write_text("8589934592")
    (v2_base / "memory.current").write_text("4294967296")

    with patch.multiple(
        "rq_geo_toolkit._system_memory",
        _CGROUP_V2_CONTROLLERS_PATH=v2_base / "cgroup.controllers",
        _CGROUP_V2_MEMORY_MAX_PATH=v2_base / "memory.max",
        _CGROUP_V2_MEMORY_CURRENT_PATH=v2_base / "memory.current",
        _CGROUP_V2_MEMORY_STAT_PATH=v2_base / "nonexistent",
    ):
        status = get_memory_status(total_bytes_override=17179869184)
    assert status.total_bytes == 17179869184
    assert status.used_bytes == 4294967296
    assert status.available_bytes == 12884901888
    assert pytest.approx(status.percent_used) == 100 * 4294967296 / 17179869184


def test_caching_source_decision(tmp_path: Path) -> None:
    """Source detection is cached but usage values are read fresh."""
    _clear_caches()

    with patch(
        "rq_geo_toolkit._system_memory._CGROUP_V2_CONTROLLERS_PATH",
        tmp_path / "nonexistent",
    ), patch(
        "rq_geo_toolkit._system_memory._CGROUP_V1_MEMORY_PATH",
        tmp_path / "nonexistent",
    ), patch(
        "rq_geo_toolkit._system_memory._get_psutil_memory_status",
        wraps=_get_psutil_memory_status,
    ) as psutil_mock:
        get_memory_status()
        get_memory_status()
        get_memory_status()

    cache_info = _detect_memory_source.cache_info()
    assert cache_info.misses == 1
    assert cache_info.hits == 2
    # Usage is read fresh on each call.
    assert psutil_mock.call_count == 3


def test_logging_reports_source_and_total(caplog: pytest.LogCaptureFixture) -> None:
    """get_memory_status logs source and total_bytes at debug level."""
    _clear_caches()
    import logging

    with caplog.at_level(logging.DEBUG, logger="rq_geo_toolkit._system_memory"):
        status = get_memory_status()

    assert any(
        f"source={status.source} total_bytes={status.total_bytes}" in record.message
        for record in caplog.records
    )
