"""Test the worker process monitoring."""

import os
import signal
import sys
from types import SimpleNamespace

import pytest

from rq_geo_toolkit.multiprocessing_utils import (
    WorkerProcess,
    raise_for_exit_code,
    run_process_with_memory_monitoring,
)


def _exit_with_error() -> None:
    os._exit(1)


def _kill_itself() -> None:
    os.kill(os.getpid(), signal.SIGKILL)


def _terminate_itself() -> None:
    os.kill(os.getpid(), signal.SIGTERM)


@pytest.mark.skipif(  # type: ignore
    sys.platform == "win32", reason="Windows exit codes don't show the cause"
)
def test_crash_is_not_reported_as_memory_error() -> None:
    """Test if a process failing on its own isn't retried with lower memory limits."""
    with pytest.raises(RuntimeError, match="exit code 1"):
        run_process_with_memory_monitoring(WorkerProcess(target=_exit_with_error))


@pytest.mark.skipif(  # type: ignore
    sys.platform == "win32", reason="SIGKILL doesn't exist on Windows"
)
def test_killed_process_is_reported_as_memory_error() -> None:
    """Test if a process killed by the system, like by the OOM killer, is a memory error."""
    with pytest.raises(MemoryError):
        run_process_with_memory_monitoring(WorkerProcess(target=_kill_itself))


@pytest.mark.skipif(  # type: ignore
    sys.platform == "win32", reason="Windows exit codes don't show the cause"
)
def test_terminated_process_is_not_reported_as_memory_error() -> None:
    """Test if a process stopped with a signal other than SIGKILL isn't a memory error."""
    with pytest.raises(RuntimeError, match="exit code -15"):
        run_process_with_memory_monitoring(WorkerProcess(target=_terminate_itself))


def test_positive_exit_code_is_memory_error_on_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test if a positive exit code is reported as a memory error on Windows."""
    monkeypatch.setattr(sys, "platform", "win32")
    with pytest.raises(MemoryError):
        raise_for_exit_code(SimpleNamespace(exitcode=1))  # type: ignore[arg-type]
