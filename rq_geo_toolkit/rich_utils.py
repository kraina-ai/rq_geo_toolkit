"""
Shared rich progress display.

Every top-level operation opens one group of progress rows in a single live display per process.
Nested operations reuse the active group, operations started from other threads get their own group
in the same live display, and child processes can forward their progress to the parent.
"""

import inspect
import multiprocessing
import os
import threading
import time
import warnings
from collections.abc import Generator, Iterable, Sized
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import timedelta
from functools import cache, cached_property, partial, wraps
from itertools import count
from typing import Any, Callable, Literal, Optional, TypeVar, Union, cast, overload

from rich import get_console
from rich.console import Console
from rich.errors import LiveError
from rich.markup import escape
from rich.progress import (
    BarColumn,
    DownloadColumn,
    MofNCompleteColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table
from rich.text import Text

__all__ = [
    "FORCE_TERMINAL",
    "VERBOSITY_MODE",
    "PoochProgressBar",
    "ProgressDisplay",
    "ProgressTask",
    "StepCounter",
    "progress_display",
    "with_progress_display",
]

VERBOSITY_MODE = Literal["silent", "transient", "verbose"]
FORCE_TERMINAL = os.getenv("FORCE_TERMINAL_MODE", "false").lower() == "true"

T = TypeVar("T")
STEP = Literal["major", "minor"]
F = TypeVar("F", bound=Callable[..., Any])

_SPINNER = "spinner"
_BAR = "bar"
_DOWNLOAD = "download"
_GROUP = "group"
# guides of `rich.tree.Tree`, the ASCII ones are for consoles that can't encode the box characters
_TREE_BRANCHES = ("├── ", "└── ")
_ASCII_TREE_BRANCHES = ("+-- ", "`-- ")
_REMOTE_UPDATE_INTERVAL_SECONDS = 0.2


class _SpeedColumn(ProgressColumn):  # type: ignore[misc]
    def render(self, task: Task) -> Text:
        if task.speed is None:
            return Text("?", style="progress.data.speed")
        elif task.speed >= 1:
            return Text(f"{task.speed:.2f} it/s")
        else:
            return Text(f"{1 / task.speed:.2f} s/it")  # noqa: FURB126


class _KindColumns(ProgressColumn):  # type: ignore[misc]
    """Render only the columns of the task kind, so other kinds don't leave empty gaps."""

    def __init__(self, columns_by_kind: dict[str, list[ProgressColumn]]) -> None:
        super().__init__()
        self.columns_by_kind = columns_by_kind

    def render(self, task: Task) -> Table:
        grid = Table.grid(padding=(0, 1))
        grid.add_row(*(column(task) for column in self.columns_by_kind[task.fields["kind"]]))
        return grid


class _DescriptionColumn(ProgressColumn):  # type: ignore[misc]
    """Render the description, prefixed with the operation label when operations share it."""

    def __init__(self, shared_live: "_SharedLive") -> None:
        super().__init__()
        self.shared_live = shared_live

    def render(self, task: Task) -> Text:
        description = Text.from_markup(task.description, style="progress.description")
        if not self.shared_live.show_labels:
            return description
        return Text.assemble((task.fields["label"], "progress.data.speed"), " ", description)


class _GroupedProgress(Progress):  # type: ignore[misc]
    """Progress rendering tasks grouped by operation, in a stable order."""

    def get_renderables(self) -> Generator[Table, None, None]:
        tasks = sorted(self.tasks, key=lambda task: (task.fields["group"], task.id))
        yield self.make_tasks_table(tasks)


@cache
def _get_console() -> Console:
    if FORCE_TERMINAL:
        return Console(force_interactive=False, force_jupyter=False, force_terminal=True)
    return get_console()


class _SharedLive:
    """Single live display shared by all operations in the current process."""

    def __init__(self) -> None:
        self.show_labels = False
        self.open_groups = 0
        self.group_indexes = count(1)
        separator = TextColumn("•")
        percentage = TextColumn("[progress.percentage]{task.percentage:>3.0f}%")
        elapsed = TimeElapsedColumn()
        remaining = TimeRemainingColumn()
        self.progress = _GroupedProgress(
            SpinnerColumn(),
            _DescriptionColumn(self),
            _KindColumns(
                {
                    _SPINNER: [separator, elapsed],
                    _BAR: [
                        percentage, BarColumn(), MofNCompleteColumn(),
                        separator, elapsed, TextColumn("<"), remaining, separator, _SpeedColumn(),
                    ],
                    _DOWNLOAD: [
                        percentage, BarColumn(), DownloadColumn(),
                        separator, elapsed, TextColumn("<"), remaining, separator,
                        TransferSpeedColumn(),
                    ],
                    _GROUP: [],
                }
            ),
            console=_get_console(),
            transient=True,
            refresh_per_second=1,
            speed_estimate_period=1800,
        )


_SHARED_LIVE_LOCK = threading.Lock()
_SHARED_LIVE: Optional[_SharedLive] = None
_ACTIVE_DISPLAY: ContextVar[Optional["ProgressDisplay"]] = ContextVar(
    "rq_geo_toolkit_active_progress_display", default=None
)
_PARENT_QUEUE: Optional["multiprocessing.Queue[Any]"] = None
_REMOTE_GROUP_IDS = count(1)
_FORMATTING_DISPLAYS = 0
_ORIGINAL_FORMATWARNING = warnings.formatwarning


class _SilentGroup:
    """Group of progress rows that discards everything."""

    def add(self, key: int, description: str, total: Optional[float], kind: str) -> None:
        pass

    def update(
        self,
        key: int,
        completed: Optional[float] = None,
        total: Optional[float] = None,
        advance: Optional[float] = None,
        description: Optional[str] = None,
    ) -> None:
        pass

    def finish(self, key: int) -> None:
        pass

    def print(self, text: str) -> None:
        pass

    def print_task(self, key: int) -> None:
        pass

    def close(self) -> None:
        pass


class _LocalGroup(_SilentGroup):
    """Group of progress rows rendered in the shared live display of this process."""

    def __init__(self, shared_live: _SharedLive, verbosity_mode: VERBOSITY_MODE) -> None:
        self.shared_live = shared_live
        self.progress = shared_live.progress
        self.verbose = verbosity_mode == "verbose"
        self.task_ids: dict[int, TaskID] = {}

    @cached_property
    def index(self) -> int:
        # assigned with the first output, so groups that show nothing don't take a number
        return next(self.shared_live.group_indexes)

    @property
    def label(self) -> str:
        return f"#{self.index}"

    def add(self, key: int, description: str, total: Optional[float], kind: str) -> None:
        if kind == _GROUP and not self.verbose:
            kind = _SPINNER
        task_id = self.progress.add_task(
            description, total=total, kind=kind, group=self.index, label=self.label
        )
        self.task_ids[key] = task_id
        if kind == _GROUP:
            # printed above its steps, so it can't wait for them like other rows
            self._stop_task(task_id)
            self.progress.update(task_id, visible=False)

    def update(
        self,
        key: int,
        completed: Optional[float] = None,
        total: Optional[float] = None,
        advance: Optional[float] = None,
        description: Optional[str] = None,
    ) -> None:
        self.progress.update(
            self.task_ids[key],
            completed=completed,
            total=total,
            advance=advance,
            description=description,
        )

    def finish(self, key: int) -> None:
        task_id = self.task_ids.pop(key)
        self._stop_task(task_id)
        self.progress.remove_task(task_id)

    def _stop_task(self, task_id: TaskID) -> None:
        task = self._get_task(task_id)
        if task.total is None:
            # mark spinners as finished, so the spinner column renders as finished
            self.progress.update(task_id, total=1, completed=1)
        self.progress.stop_task(task_id)
        if self.verbose and task.visible:
            self.progress.console.print(self.progress.make_tasks_table([task]))

    def print(self, text: str) -> None:
        if self.shared_live.show_labels:
            text = f"[progress.data.speed]{self.label}[/] {text}"
        self.progress.console.print(text)

    def print_task(self, key: int) -> None:
        self.progress.console.print(
            self.progress.make_tasks_table([self._get_task(self.task_ids[key])])
        )

    def close(self) -> None:
        global _SHARED_LIVE  # noqa: PLW0603
        # only rows of a crashed child process are left here, and they didn't finish
        for task_id in self.task_ids.values():
            self.progress.remove_task(task_id)
        with _SHARED_LIVE_LOCK:
            self.shared_live.open_groups -= 1
            if self.shared_live.open_groups == 0:
                # `Progress.stop` adds an empty line in non-interactive consoles, which only
                # makes sense for non-transient progress
                self.progress.live.stop()
                _SHARED_LIVE = None

    def _get_task(self, task_id: TaskID) -> Task:
        return next(task for task in self.progress.tasks if task.id == task_id)


class _PlainGroup(_SilentGroup):
    """Group of a child process not attached to a display, without any live rendering."""

    def __init__(self, verbosity_mode: VERBOSITY_MODE) -> None:
        self.console = _get_console()
        self.label = f"pid {os.getpid()}:"
        self.verbose = verbosity_mode == "verbose"
        self.rows: dict[int, tuple[str, float]] = {}

    def add(self, key: int, description: str, total: Optional[float], kind: str) -> None:
        if kind != _GROUP:
            self.rows[key] = (description, time.monotonic())
        elif self.verbose:
            self.print(description)

    def update(
        self,
        key: int,
        completed: Optional[float] = None,
        total: Optional[float] = None,
        advance: Optional[float] = None,
        description: Optional[str] = None,
    ) -> None:
        if description is not None:
            self.rows[key] = (description, self.rows[key][1])

    def finish(self, key: int) -> None:
        if self.verbose and key in self.rows:
            self.print_task(key)
        self.rows.pop(key, None)

    def print(self, text: str) -> None:
        # single lines, so processes writing to the same console don't overwrite each other
        self.console.print(f"[progress.data.speed]{self.label}[/] {text}")

    def print_task(self, key: int) -> None:
        description, start_time = self.rows[key]
        elapsed_time = timedelta(seconds=int(time.monotonic() - start_time))
        self.print(f"{description} • {elapsed_time}")


class _RemoteGroup(_SilentGroup):
    """Group of progress rows forwarded from a child process to the parent display."""

    def __init__(
        self, queue: "multiprocessing.Queue[Any]", verbosity_mode: VERBOSITY_MODE
    ) -> None:
        self.queue = queue
        self.group_id = f"{os.getpid()}-{next(_REMOTE_GROUP_IDS)}"
        self.completed: dict[int, float] = {}
        self.sent_at: dict[int, float] = {}
        self._send("open", verbosity_mode)

    def _send(self, method: str, *args: Any) -> None:
        self.queue.put((self.group_id, method, args))

    def add(self, key: int, description: str, total: Optional[float], kind: str) -> None:
        self.completed[key] = 0
        self._send("add", key, description, total, kind)

    def update(
        self,
        key: int,
        completed: Optional[float] = None,
        total: Optional[float] = None,
        advance: Optional[float] = None,
        description: Optional[str] = None,
    ) -> None:
        if advance is not None:
            self.completed[key] += advance
        if completed is not None:
            self.completed[key] = completed

        # throttle frequent updates, but always send changes of total and description
        now = time.monotonic()
        if (
            total is None is description
            and now - self.sent_at.get(key, 0) < _REMOTE_UPDATE_INTERVAL_SECONDS
        ):
            return
        self.sent_at[key] = now
        self._send("update", key, self.completed[key], total, None, description)

    def finish(self, key: int) -> None:
        self.sent_at.pop(key, None)
        self._send("update", key, self.completed.pop(key), None, None, None)
        self._send("finish", key)

    def print(self, text: str) -> None:
        self._send("print", text)

    def print_task(self, key: int) -> None:
        self._send("update", key, self.completed[key], None, None, None)
        self._send("print_task", key)

    def close(self) -> None:
        self._send("close")


def _open_local_group(verbosity_mode: VERBOSITY_MODE) -> _SilentGroup:
    global _SHARED_LIVE  # noqa: PLW0603
    with _SHARED_LIVE_LOCK:
        if _SHARED_LIVE is None:
            shared_live = _SharedLive()
            try:
                shared_live.progress.start()
            except LiveError:
                # another live display (outside of this module) is already active
                return _PlainGroup(verbosity_mode)
            _SHARED_LIVE = shared_live

        _SHARED_LIVE.open_groups += 1
        if _SHARED_LIVE.open_groups > 1:
            _SHARED_LIVE.show_labels = True
        return _LocalGroup(_SHARED_LIVE, verbosity_mode)


def _open_group(verbosity_mode: VERBOSITY_MODE) -> _SilentGroup:
    if verbosity_mode == "silent":
        return _SilentGroup()
    if multiprocessing.parent_process() is not None:
        # only the parent display can render live progress from child processes
        if _PARENT_QUEUE is None:
            return _PlainGroup(verbosity_mode)
        return _RemoteGroup(_PARENT_QUEUE, verbosity_mode)
    return _open_local_group(verbosity_mode)


def _attach_to_parent(queue: Optional["multiprocessing.Queue[Any]"]) -> None:
    global _PARENT_QUEUE  # noqa: PLW0603
    _PARENT_QUEUE = queue


def _listen_to_children(queue: "multiprocessing.Queue[Any]") -> None:
    groups: dict[str, _SilentGroup] = {}
    try:
        for group_id, method, args in iter(queue.get, None):
            if method == "open":
                groups[group_id] = _open_group(*args)
            elif method == "close":
                groups.pop(group_id).close()
            else:
                getattr(groups[group_id], method)(*args)
    finally:
        # a crashed child process doesn't close its group
        for group in groups.values():
            group.close()


class ProgressTask:
    """Single progress row."""

    def __init__(self, group: _SilentGroup, key: int) -> None:
        """
        Create a handle for a progress row.

        Use `ProgressDisplay.bar` instead.
        """
        self._group = group
        self._key = key

    def update(
        self,
        completed: Optional[float] = None,
        *,
        total: Optional[float] = None,
        advance: Optional[float] = None,
        description: Optional[str] = None,
    ) -> None:
        """
        Update the progress row.

        Args:
            completed (Optional[float], optional): Number of completed steps. Defaults to None.
            total (Optional[float], optional): Total number of steps. Defaults to None.
            advance (Optional[float], optional): Number of steps to advance. Defaults to None.
            description (Optional[str], optional): New description. Defaults to None.
        """
        self._group.update(self._key, completed, total, advance, description)

    def track(
        self, iterable: Iterable[T], total: Optional[float] = None
    ) -> Generator[T, None, None]:
        """
        Iterate over values and advance the progress row after each one.

        Args:
            iterable (Iterable[T]): Values to iterate over.
            total (Optional[float], optional): Total number of steps. Defaults to the length of
                the iterable, if available.

        Yields:
            T: Values from the iterable.
        """
        if total is None and isinstance(iterable, Sized):
            total = len(iterable)
        if total is not None:
            self.update(total=total)
        for value in iterable:
            yield value
            self.update(advance=1)

    def print_progress(self) -> None:
        """Print the current state of the progress row as a permanent line."""
        self._group.print_task(self._key)


class PoochProgressBar:
    """
    Progress bar object for the `progressbar` argument of the `pooch` library.

    Unlike `tqdm`, it doesn't reset the elapsed time when `pooch` refills the bar after the
    download, so the final transfer speed stays correct.
    """

    def __init__(self, display: "ProgressDisplay", description: str) -> None:
        """
        Create a pooch progress bar.

        Use `ProgressDisplay.pooch_progressbar` instead.
        """
        self._display = display
        self.description = description
        self._key: Optional[int] = None
        self._total: Optional[float] = None
        self._completed: float = 0

    def __bool__(self) -> bool:
        """Report a progress bar of a silent display as disabled."""
        return not self._display.is_silent

    @property
    def total(self) -> Optional[float]:
        """Total number of bytes to download."""
        return self._total

    @total.setter
    def total(self, value: Optional[float]) -> None:
        self._total = value or None
        self._completed = 0
        if self._key is None:
            self._key = self._display._add_task(_DOWNLOAD, self.description, self._total)
        else:
            self._display._group.update(self._key, 0, self._total)

    def update(self, n: int) -> None:
        """Advance the number of downloaded bytes."""
        self._completed += n
        if self._key is not None:
            completed = self._completed
            if self._total is not None:
                completed = min(completed, self._total)
            self._display._group.update(self._key, completed)

    def reset(self) -> None:
        """Reset the number of downloaded bytes without resetting the elapsed time."""
        self._completed = 0

    def set_description(self, description: str) -> None:
        """Change the description, e.g. to the name of the next downloaded file."""
        self.description = description
        if self._key is not None:
            self._display._group.update(self._key, description=description)

    def close(self) -> None:
        """Finish the current download."""
        if self._key is not None:
            self._display._group.finish(self._key)
            self._key = None


class StepCounter:
    """Numbering of major steps of an operation and their minor steps."""

    def __init__(self, total_steps: int) -> None:
        """
        Create a step counter.

        Args:
            total_steps (int): Number of major steps of the operation.
        """
        self.total_steps = total_steps
        self.prefix = ""
        self.major_step_number = 0
        self.minor_step_number: Optional[int] = None
        self.minor_steps_total: Optional[int] = None
        self._nesting_depth = 0

    @property
    def is_new(self) -> bool:
        """Whether no step was counted yet."""
        return self.major_step_number == 0 and self.minor_step_number is None

    @property
    def step_number(self) -> str:
        """Unique number of the current step, like `2` or `2.1`."""
        if self.minor_step_number is None:
            return str(self.major_step_number)
        return f"{self.major_step_number}.{self.minor_step_number}"

    @property
    def current_step(self) -> str:
        """Label of the current step, like `[2/16]` or `├── [1/7]`."""
        if self.minor_step_number is None:
            width = len(str(self.total_steps))
            return f"{self.prefix}[{self.major_step_number: >{width}}/{self.total_steps}]"

        total = self.minor_steps_total
        branches = _ASCII_TREE_BRANCHES if _get_console().options.ascii_only else _TREE_BRANCHES
        branch = branches[self.minor_step_number == total]
        return f"{self.prefix}{branch}[{self.minor_step_number: >{len(str(total))}}/{total}]"

    def add_steps(self, count: int) -> None:
        """
        Extend the total when the number of steps is known only during the operation.

        A nested operation runs as minor steps of the outer one, so it extends their total.
        """
        if self._nesting_depth:
            self.add_minor_steps(count)
        else:
            self.total_steps += count

    def add_minor_steps(self, count: int) -> None:
        """Extend the total of minor steps of the current major step."""
        self.minor_steps_total = (self.minor_steps_total or 0) + count

    def next_step(self, step: STEP = "major", minor_steps: Optional[int] = None) -> str:
        """
        Count the next step.

        Args:
            step (Literal["major", "minor"], optional): Type of the step. Major steps of
                a nested operation are counted as minor steps. Defaults to "major".
            minor_steps (Optional[int], optional): Number of minor steps of a new major step.
                Defaults to None.

        Returns:
            str: Label of the counted step.
        """
        if step == "major" and not self._nesting_depth:
            self.major_step_number += 1
            self.minor_step_number = None
            self.minor_steps_total = minor_steps
        else:
            self.minor_step_number = (self.minor_step_number or 0) + 1
        return self.current_step

    def reset(self, prefix: str = "") -> None:
        """
        Start counting from the beginning, e.g. for the next file.

        Args:
            prefix (str, optional): Prefix of the step labels. Defaults to "".
        """
        self.prefix = prefix
        self.major_step_number = 0
        self.minor_step_number = None
        self.minor_steps_total = None

    @contextmanager
    def _nested_operation(self, total_steps: int) -> Generator[None, None, None]:
        # an operation started before the first step continues the numbering, a later one
        # joins the current step group or takes the next major step, and its steps become
        # minor steps of it
        if self.is_new:
            yield
            return
        if self.minor_steps_total is None:
            self.next_step(minor_steps=0)
        self.add_minor_steps(total_steps)
        self._nesting_depth += 1
        try:
            yield
        finally:
            self._nesting_depth -= 1


class ProgressDisplay:
    """Group of progress rows belonging to a single top-level operation."""

    def __init__(self, group: _SilentGroup) -> None:
        """
        Create a progress display.

        Use the `progress_display` context manager instead.
        """
        self._group = group
        self._keys = count()
        self._used = False
        self._listener: Optional[tuple[multiprocessing.Queue[Any], threading.Thread]] = None
        self._step_group_key: Optional[int] = None
        self.steps: Optional[StepCounter] = None
        self.pid = os.getpid()

    @property
    def is_silent(self) -> bool:
        """Whether all progress is discarded."""
        return type(self._group) is _SilentGroup

    def _add_task(
        self,
        kind: str,
        description: str,
        total: Optional[float],
        step: Optional[STEP] = None,
    ) -> int:
        if step is not None and self.steps is not None:
            description = f"{self.steps.next_step(step)} {description}"
            if self.steps.minor_steps_total is None:
                self._close_step_group()
        key = next(self._keys)
        self._group.add(key, description, total, kind)
        self._used = True
        return key

    def _close_step_group(self) -> None:
        if self._step_group_key is not None:
            self._group.finish(self._step_group_key)
            self._step_group_key = None

    def step_group(self, description: str, total: int = 0) -> None:
        """
        Start the next major step, which consists of minor steps shown under it.

        The group stays open until the next major step or the end of the operation. In a nested
        operation there is no new row, because the group is already a minor step of the outer
        operation, so only the total of minor steps is extended.

        Args:
            description (str): Name of the major step.
            total (int, optional): Number of its minor steps. Steps of operations nested in
                the group are added automatically. Defaults to 0.
        """
        if self.steps is None:
            return
        if self.steps._nesting_depth:
            self.steps.add_minor_steps(total - 1)
            return
        self._close_step_group()
        label = self.steps.next_step(minor_steps=total)
        self._step_group_key = self._add_task(_GROUP, f"{label} {description}", None)

    @contextmanager
    def spinner(
        self, description: str, step: Optional[STEP] = None
    ) -> Generator[None, None, None]:
        """
        Show a spinner with the elapsed time until the context exits.

        Args:
            description (str): Spinner description.
            step (Optional[Literal["major", "minor"]], optional): Count it as the next major
                or minor step of the operation and prefix the description with the step
                number. Works only when the operation has `total_steps`. Defaults to None.
        """
        key = self._add_task(_SPINNER, description, None, step)
        try:
            yield
        finally:
            self._group.finish(key)

    @contextmanager
    def bar(
        self,
        description: str,
        total: Optional[float] = None,
        step: Optional[STEP] = None,
    ) -> Generator[ProgressTask, None, None]:
        """
        Show a progress bar until the context exits.

        Args:
            description (str): Progress bar description.
            total (Optional[float], optional): Total number of steps. Can be set later with
                `ProgressTask.update`. Defaults to None.
            step (Optional[Literal["major", "minor"]], optional): Count it as the next major
                or minor step of the operation and prefix the description with the step
                number. Works only when the operation has `total_steps`. Defaults to None.

        Yields:
            ProgressTask: Handle for updating the progress bar.
        """
        key = self._add_task(_BAR, description, total, step)
        try:
            yield ProgressTask(self._group, key)
        finally:
            self._group.finish(key)

    def pooch_progressbar(self, description: str) -> PoochProgressBar:
        """
        Create a progress bar for the `progressbar` argument of the `pooch` library.

        Args:
            description (str): Download description, e.g. the file name.

        Returns:
            PoochProgressBar: Progress bar object. It is falsy when the display is silent.
        """
        return PoochProgressBar(self, description)

    def print(self, text: str) -> None:
        """
        Print a permanent line above the progress rows.

        Args:
            text (str): Text with optional rich markup.
        """
        self._group.print(text)

    def worker_initializer(self) -> Callable[[], None]:
        """
        Get an initializer that attaches child processes to this display.

        Pass it as the `initializer` of a `ProcessPoolExecutor` or a `multiprocessing.Pool`,
        or call it at the start of a `multiprocessing.Process` target. Progress of operations
        running in attached child processes is shown as separate groups in this display.
        Child processes that aren't attached print plain lines prefixed with `pid N:`,
        unless they run in the silent verbosity mode. A pool must not outlive this display,
        because its workers stay attached to it.

        Returns:
            Callable[[], None]: Picklable initializer.
        """
        if not isinstance(self._group, _LocalGroup):
            return partial(_attach_to_parent, None)
        if self._listener is None:
            queue: multiprocessing.Queue[Any] = multiprocessing.get_context("spawn").Queue()
            thread = threading.Thread(target=_listen_to_children, args=(queue,), daemon=True)
            thread.start()
            self._listener = (queue, thread)
        return partial(_attach_to_parent, self._listener[0])

    def _close(self) -> None:
        self._close_step_group()
        if self._listener is not None:
            # process all messages from child processes before closing this group
            queue, thread = self._listener
            queue.put(None)
            thread.join()
            self._listener = None
        self._group.close()


def _format_warning(
    message: Union[Warning, str],
    category: type[Warning],
    filename: str,
    lineno: int,
    line: Optional[str] = None,
) -> str:
    text = f"{category.__name__}: {message}"
    display = _ACTIVE_DISPLAY.get()
    if display is None or display.pid != os.getpid():
        return _ORIGINAL_FORMATWARNING(message, category, filename, lineno, line)
    if display.is_silent:
        return f"{text}\n"
    display.print(f"[yellow]{category.__name__}:[/] {escape(str(message))}")
    return ""


@contextmanager
def _display_warnings() -> Generator[None, None, None]:
    # only called for printed warnings, so filters and recorded warnings keep working
    global _FORMATTING_DISPLAYS, _ORIGINAL_FORMATWARNING  # noqa: PLW0603
    with _SHARED_LIVE_LOCK:
        if not _FORMATTING_DISPLAYS:
            _ORIGINAL_FORMATWARNING = warnings.formatwarning
            warnings.formatwarning = _format_warning
        _FORMATTING_DISPLAYS += 1
    try:
        yield
    finally:
        with _SHARED_LIVE_LOCK:
            _FORMATTING_DISPLAYS -= 1
            if not _FORMATTING_DISPLAYS and warnings.formatwarning is _format_warning:
                warnings.formatwarning = _ORIGINAL_FORMATWARNING


@contextmanager
def progress_display(
    verbosity_mode: VERBOSITY_MODE = "transient",
    show_elapsed_time: bool = False,
    total_steps: Optional[int] = None,
) -> Generator[ProgressDisplay, None, None]:
    """
    Open a progress display for an operation, or reuse the active one.

    Only the outermost call in the current thread opens a new group of progress rows.
    Nested calls reuse it, so a whole operation is rendered in a single live display.

    Args:
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Ignored in nested calls.
            Defaults to "transient".
        show_elapsed_time (bool, optional): Print the total elapsed time after the outermost
            operation finishes, if it reported any progress. Ignored in nested calls.
            Defaults to False.
        total_steps (Optional[int], optional): Number of major steps of the operation, which
            enables step numbers in `ProgressDisplay.spinner` and `ProgressDisplay.bar`.
            A nested operation started before the first step of the outer one continues its
            numbering. A nested operation started later takes the next major step of the outer
            operation and its steps are counted as minor steps. Defaults to None.

    Yields:
        ProgressDisplay: Progress display.
    """
    active_display = _ACTIVE_DISPLAY.get()
    # a forked child process inherits the display of its parent, but can't render to it
    if active_display is not None and active_display.pid == os.getpid():
        if total_steps is None:
            yield active_display
        elif active_display.steps is not None:
            with active_display.steps._nested_operation(total_steps):
                yield active_display
        else:
            active_display.steps = StepCounter(total_steps)
            try:
                yield active_display
            finally:
                active_display._close_step_group()
                active_display.steps = None
        return

    display = ProgressDisplay(_open_group(verbosity_mode))
    if total_steps is not None:
        display.steps = StepCounter(total_steps)
    token = _ACTIVE_DISPLAY.set(display)
    start_time = time.monotonic()
    try:
        with _display_warnings():
            yield display
        if show_elapsed_time and display._used:
            elapsed_time = timedelta(seconds=int(time.monotonic() - start_time))
            display.print(f"Finished operation in [progress.elapsed]{elapsed_time}")
    finally:
        _ACTIVE_DISPLAY.reset(token)
        display._close()


@overload
def with_progress_display(func: F) -> F: ...


@overload
def with_progress_display(*, total_steps: Optional[int] = None) -> Callable[[F], F]: ...


def with_progress_display(
    func: Optional[F] = None, *, total_steps: Optional[int] = None
) -> Union[F, Callable[[F], F]]:
    """
    Run the decorated operation in a progress display and print its total elapsed time.

    The verbosity mode is read from the `verbosity_mode` argument of the call, or from
    the `verbosity_mode` attribute of the object for methods without this argument.
    Decorated operations called from other decorated operations reuse their display.
    Use as `@with_progress_display` or `@with_progress_display(total_steps=...)`.

    Args:
        func (Optional[F], optional): Operation to decorate. Defaults to None.
        total_steps (Optional[int], optional): Number of major steps of the operation.
            See `progress_display` for details. Defaults to None.

    Returns:
        Union[F, Callable[[F], F]]: Decorated operation, or a decorator when called
            with arguments only.
    """
    if func is None:
        return partial(with_progress_display, total_steps=total_steps)

    signature = inspect.signature(func)

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        arguments = signature.bind(*args, **kwargs)
        arguments.apply_defaults()
        verbosity_mode = arguments.arguments.get("verbosity_mode") or getattr(
            arguments.arguments.get("self"), "verbosity_mode", "transient"
        )
        with progress_display(
            cast("VERBOSITY_MODE", verbosity_mode),
            show_elapsed_time=True,
            total_steps=total_steps,
        ):
            return func(*args, **kwargs)

    return cast("F", wrapper)
