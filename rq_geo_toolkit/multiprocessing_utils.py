"""Utilities for multiprocessing."""

import multiprocessing
import traceback
from time import sleep
from typing import Any, Optional

import psutil

from rq_geo_toolkit.constants import MEMORY_1GB


class WorkerProcess(multiprocessing.Process):
    """Dedicated class for running processes with catching exceptions."""

    def __init__(self, *args: Any, **kwargs: Any):
        """Initialize the process."""
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception: Optional[tuple[Exception, str]] = None

    def run(self) -> None:  # pragma: no cover
        """Run the process."""
        try:
            multiprocessing.Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb: str = traceback.format_exc()
            self._cconn.send((e, tb))

    @property
    def exception(self) -> Optional[tuple[Exception, str]]:
        """Return the exception if occured."""
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


def run_process_with_memory_monitoring(process: WorkerProcess) -> None:
    """
    Start a process and monitor the memory usage.

    Raises exceptions reported within process.
    """
    actual_memory = psutil.virtual_memory()
    process.start()
    percentage_threshold = 95
    if (actual_memory.total * 0.05) > MEMORY_1GB:  # pragma: no cover
        percentage_threshold = 100 * (actual_memory.total - MEMORY_1GB) / actual_memory.total

    sleep_time = 0.1
    while process.is_alive():
        actual_memory = psutil.virtual_memory()
        if actual_memory.percent > percentage_threshold:  # pragma: no cover
            process.terminate()
            process.join()
            raise MemoryError()

        sleep(sleep_time)
        sleep_time = min(sleep_time + 0.1, 1.0)

    if process.exception:
        error, traceback = process.exception
        msg = f"{error}\n\nOriginal {traceback}"
        raise type(error)(msg)

    if process.exitcode != 0:
        raise MemoryError()
