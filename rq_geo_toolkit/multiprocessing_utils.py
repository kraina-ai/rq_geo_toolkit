"""Utilities for multiprocessing."""

import multiprocessing
import traceback
from typing import Any, Optional


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
