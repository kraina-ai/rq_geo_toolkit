"""Test the shared progress display."""

import io
import multiprocessing
import re
import sys
import threading
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Callable, Optional
from unittest.mock import patch

import pytest
from rich.console import Console

from rq_geo_toolkit import rich_utils
from rq_geo_toolkit.rich_utils import VERBOSITY_MODE, progress_display, with_progress_display


@pytest.fixture  # type: ignore
def console_output(monkeypatch: pytest.MonkeyPatch) -> Callable[[], str]:
    """Record the shared display output as plain text."""
    file = io.StringIO()
    console = Console(file=file, force_terminal=True, color_system=None, width=200)
    monkeypatch.setattr(rich_utils, "_get_console", lambda: console)
    return lambda: re.sub(r"\x1b\[[0-9;?]*[A-Za-z]", "", file.getvalue())


def test_nested_calls_reuse_display(console_output: Callable[[], str]) -> None:
    """Test if nested calls reuse the outer display and keep a single live display."""
    with progress_display("verbose") as outer:
        with progress_display("silent") as inner:
            assert inner is outer
            with inner.spinner("Nested step"):
                pass

    assert "Nested step" in console_output()
    assert rich_utils._SHARED_LIVE is None


def test_silent_display(console_output: Callable[[], str]) -> None:
    """Test if a silent display doesn't print anything."""
    with progress_display("silent", show_elapsed_time=True) as display:
        assert display.is_silent
        assert not display.pooch_progressbar("file.txt")
        with display.bar("Hidden step", total=1) as task:
            task.update(1)

    assert console_output() == ""


def test_elapsed_time_only_after_progress(console_output: Callable[[], str]) -> None:
    """Test if the elapsed time is printed only when an operation reported progress."""
    with progress_display("transient", show_elapsed_time=True):
        pass
    assert "Finished operation" not in console_output()

    with progress_display("transient", show_elapsed_time=True) as display:
        with display.spinner("Step"):
            pass
    assert "Finished operation in" in console_output()


def test_decorator_reads_verbosity_mode(console_output: Callable[[], str]) -> None:
    """Test if the decorator uses the default and passed verbosity modes and nests once."""

    @with_progress_display
    def inner(verbosity_mode: VERBOSITY_MODE = "silent") -> None:
        with progress_display() as display, display.spinner("Inner step"):
            pass

    @with_progress_display
    def outer(verbosity_mode: VERBOSITY_MODE) -> None:
        inner()
        inner(verbosity_mode="silent")

    inner()
    assert console_output() == ""

    outer("verbose")
    assert "Inner step" in console_output()
    assert console_output().count("Finished operation in") == 1


class _Reader:
    verbosity_mode: VERBOSITY_MODE = "verbose"

    @with_progress_display
    def run(self) -> None:
        with progress_display() as display, display.spinner("Method step"):
            pass


def test_decorator_reads_verbosity_mode_from_object(console_output: Callable[[], str]) -> None:
    """Test if the decorator falls back to the `verbosity_mode` attribute of the object."""
    _Reader().run()
    assert "Method step" in console_output()


def _step_labels(output: str) -> list[str]:
    labels = re.findall(r"((?:[├└]── )?\[\s*\d+/\d+\] \w+)", output)
    return list(dict.fromkeys(re.sub(r"\s+", " ", label) for label in labels))


def test_steps_numbering(console_output: Callable[[], str]) -> None:
    """Test if major and minor steps are numbered and rows without a step are not."""
    with progress_display("verbose", total_steps=3) as display:
        with display.spinner("First", step="major"):
            pass
        display.step_group("Group", total=2)
        with display.bar("Second", total=1, step="minor") as task:
            task.update(1)
        assert display.steps and display.steps.step_number == "2.1"
        with display.spinner("Third", step="minor"):
            pass
        with display.spinner("Unnumbered"):
            pass

    assert _step_labels(console_output()) == [
        "[1/3] First",
        "[2/3] Group",
        "├── [1/2] Second",
        "└── [2/2] Third",
    ]
    assert re.search(r"\s{2}Unnumbered", console_output())


def test_operation_labels_have_different_colors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test if labels of concurrent operations can be told apart by color."""
    file = io.StringIO()
    console = Console(file=file, force_terminal=True, color_system="standard", width=100)
    monkeypatch.setattr(rich_utils, "_get_console", lambda: console)

    def second() -> None:
        with progress_display("verbose") as display, display.spinner("Second"):
            pass

    with progress_display("verbose") as display, display.spinner("First"):
        thread = threading.Thread(target=second)
        thread.start()
        thread.join()

    assert "\x1b[36m#1\x1b[0m" in file.getvalue()
    assert "\x1b[35m#2\x1b[0m" in file.getvalue()


def test_steps_use_ascii_in_console_without_unicode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test if a console that can't encode box characters gets ASCII labels."""
    file = io.TextIOWrapper(io.BytesIO(), encoding="cp1252")
    console = Console(file=file, width=100)
    monkeypatch.setattr(rich_utils, "_get_console", lambda: console)
    with progress_display("verbose", total_steps=1) as display:
        display.step_group("Group", total=2)
        for name in ("First", "Second"):
            with display.spinner(name, step="minor"):
                pass

    file.flush()
    output = file.buffer.getvalue()
    assert b"+-- [1/2] First" in output
    assert b"`-- [2/2] Second" in output


def test_step_group_row_stays_until_next_major_step(console_output: Callable[[], str]) -> None:
    """Test if a transient step group is shown above its minor steps until the next major step."""
    with progress_display("transient", total_steps=2) as display:
        display.step_group("Group", total=1)
        with display.spinner("Minor", step="minor"):
            assert rich_utils._SHARED_LIVE
            rows = [task.description for task in rich_utils._SHARED_LIVE.progress.tasks]
            assert rows == ["[1/2] Group", "└── [1/1] Minor"]
        with display.spinner("Major", step="major"):
            rows = [task.description for task in rich_utils._SHARED_LIVE.progress.tasks]
            assert rows == ["[2/2] Major"]


@pytest.mark.parametrize("group_total", [None, 0])  # type: ignore
def test_minor_step_without_known_total(
    console_output: Callable[[], str], group_total: Optional[int]
) -> None:
    """Test if a minor step without a known total of minor steps uses the major total."""
    with progress_display("verbose", total_steps=2) as display:
        if group_total is None:
            with display.spinner("Major", step="major"):
                pass
        else:
            display.step_group("Major", total=group_total)
        with display.spinner("Minor", step="minor"):
            pass

    assert "[1.1/2] Minor" in console_output()


def test_nested_operation_joins_step_group(console_output: Callable[[], str]) -> None:
    """Test if steps and step groups of a nested operation extend the open step group."""

    @with_progress_display(total_steps=2)
    def nested(verbosity_mode: VERBOSITY_MODE = "verbose") -> None:
        with progress_display() as display:
            display.step_group("Skipped", total=2)
            with display.spinner("First", step="minor"):
                pass
            with display.spinner("Second", step="minor"):
                pass
            with display.spinner("Third", step="major"):
                pass

    with progress_display("verbose", total_steps=2) as display:
        with display.spinner("Before", step="major"):
            pass
        display.step_group("Group")
        nested()

    assert _step_labels(console_output()) == [
        "[1/2] Before",
        "[2/2] Group",
        "├── [1/3] First",
        "├── [2/3] Second",
        "└── [3/3] Third",
    ]


@with_progress_display(total_steps=2)
def _inner_operation(verbosity_mode: VERBOSITY_MODE = "verbose") -> None:
    with progress_display() as display:
        for name in ("Inner", "Second"):
            with display.spinner(name, step="major"):
                pass


@with_progress_display(total_steps=5)
def _outer_operation(verbosity_mode: VERBOSITY_MODE, inner_first: bool) -> None:
    with progress_display() as display:
        if inner_first:
            _inner_operation()
        with display.spinner("Outer", step="major"):
            pass
        _inner_operation()
        with display.spinner("Last", step="major"):
            pass


def test_nested_operation_steps(console_output: Callable[[], str]) -> None:
    """Test if nested operations continue the numbering or become minor steps."""
    _outer_operation("verbose", inner_first=True)

    assert _step_labels(console_output()) == [
        "[1/5] Inner",
        "[2/5] Second",
        "[3/5] Outer",
        "[4/5] Nested",
        "├── [1/2] Inner",
        "└── [2/2] Second",
        "[5/5] Last",
    ]


@with_progress_display(total_steps=1)
def _dynamic_operation(verbosity_mode: VERBOSITY_MODE = "verbose") -> None:
    with progress_display() as display:
        assert display.steps
        display.steps.add_steps(1)
        with display.spinner("First", step="major"):
            pass
        with display.spinner("Second", step="major"):
            pass


def test_add_steps_extends_total(console_output: Callable[[], str]) -> None:
    """Test if an outermost operation extends major steps and a nested one minor steps."""
    _dynamic_operation()
    assert _step_labels(console_output()) == ["[1/2] First", "[2/2] Second"]

    @with_progress_display(total_steps=2)
    def outer(verbosity_mode: VERBOSITY_MODE = "verbose") -> None:
        with progress_display() as display, display.spinner("Before", step="major"):
            pass
        _dynamic_operation()

    outer()
    assert _step_labels(console_output())[2:] == [
        "[1/2] Before",
        "[2/2] Nested",
        "├── [1/2] First",
        "└── [2/2] Second",
    ]


def test_steps_without_total_are_not_numbered(console_output: Callable[[], str]) -> None:
    """Test if steps are ignored when the operation has no total number of steps."""
    with progress_display("verbose") as display, display.spinner("Plain", step="major"):
        pass

    assert not _step_labels(console_output())
    assert display.steps is None


def test_no_empty_line_in_non_interactive_console(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test if a transient display leaves only the elapsed time in a non-interactive console."""
    file = io.StringIO()
    console = Console(file=file, force_terminal=False, force_interactive=False)
    monkeypatch.setattr(rich_utils, "_get_console", lambda: console)

    with progress_display("transient", show_elapsed_time=True) as display:
        with display.spinner("Step"):
            pass

    assert re.fullmatch(r"Finished operation in 0:00:0\d\n", file.getvalue())


def test_threads_share_display_in_stable_order(console_output: Callable[[], str]) -> None:
    """Test if operations from many threads share one display without errors."""
    first_started = threading.Event()
    second_finished = threading.Event()

    def first() -> None:
        with progress_display("verbose") as display, display.spinner("First operation"):
            first_started.set()
            second_finished.wait(timeout=10)

    def second() -> None:
        first_started.wait(timeout=10)
        with progress_display("verbose") as display, display.spinner("Second operation"):
            pass
        second_finished.set()

    threads = [threading.Thread(target=first), threading.Thread(target=second)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    output = console_output()
    assert re.search(r"#2\s+Second operation", output)
    assert re.search(r"#1\s+First operation", output)
    assert rich_utils._SHARED_LIVE is None


def test_operation_without_output_takes_no_number(console_output: Callable[[], str]) -> None:
    """Test if an operation that only holds the display open doesn't take a label number."""

    def operation() -> None:
        with progress_display("verbose") as display, display.spinner("Visible operation"):
            pass

    with progress_display("transient"):
        thread = threading.Thread(target=operation)
        thread.start()
        thread.join()

    assert re.search(r"#1\s+Visible operation", console_output())


def _queue_with_open_row(*messages: Any) -> "multiprocessing.Queue[Any]":
    queue: multiprocessing.Queue[Any] = multiprocessing.get_context("spawn").Queue()
    queue.put(("group", "open", ("verbose",)))
    queue.put(("group", "add", (1, "Crashed step", None, rich_utils._SPINNER)))
    for message in messages:
        queue.put(message)
    return queue


def test_crashed_child_process_does_not_keep_display_open(
    console_output: Callable[[], str],
) -> None:
    """Test if a group of a child process that never closed it is closed with the listener."""
    with patch.object(rich_utils._LocalGroup, "finish") as finish:
        rich_utils._listen_to_children(_queue_with_open_row(None))
    finish.assert_not_called()
    assert rich_utils._SHARED_LIVE is None


def test_failed_listener_does_not_keep_display_open(console_output: Callable[[], str]) -> None:
    """Test if groups are closed when a message from a child process can't be handled."""
    with pytest.raises(KeyError):
        rich_utils._listen_to_children(_queue_with_open_row(("group", "update", (2,))))
    assert rich_utils._SHARED_LIVE is None


def test_warnings_use_display_only_during_operation(
    console_output: Callable[[], str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test if the display prints warnings during an operation and they can still be caught."""
    arguments = ("Message [x]", UserWarning, "file.py", 1)
    # pytest records warnings, so restore the path of a warning printed to the console
    monkeypatch.setattr(
        warnings,
        "showwarning",
        lambda message, category, filename, lineno, file=None, line=None: warnings.formatwarning(
            message, category, filename, lineno, line
        ),
    )
    with progress_display("silent"):
        assert warnings.formatwarning(*arguments) == "UserWarning: Message [x]\n"

    with progress_display("transient"):
        assert warnings.formatwarning(*arguments) == ""
        warnings.warn("Uncaptured", UserWarning, stacklevel=1)
        with ThreadPoolExecutor(max_workers=1) as executor:
            # a thread without the display isn't a part of this operation
            assert "file.py" in executor.submit(warnings.formatwarning, *arguments).result()
        with pytest.warns(UserWarning, match="Caught"):
            warnings.warn("Caught", UserWarning, stacklevel=1)

    with progress_display("transient") as display:
        display.pid = -1  # like a display inherited through fork
        assert "file.py" in warnings.formatwarning(*arguments)

    assert "UserWarning: Message [x]" in console_output()
    assert "UserWarning: Uncaptured" in console_output()
    assert "Caught" not in console_output()
    assert "file.py" in warnings.formatwarning(*arguments)


def test_pooch_progressbar_keeps_elapsed_time(console_output: Callable[[], str]) -> None:
    """Test if refilling the bar after a download keeps the elapsed time and final size."""
    with progress_display("verbose") as display:
        progressbar = display.pooch_progressbar("file.osm.pbf")
        # the same sequence of calls as in pooch.HTTPDownloader
        progressbar.total = 1000
        progressbar.update(600)
        time.sleep(1.1)
        progressbar.update(600)
        progressbar.reset()
        progressbar.update(1000)
        progressbar.close()

    output = console_output()
    assert re.search(r"file\.osm\.pbf\s+100%.*1\.0/1\.0 kB.*0:00:01", output)


def _run_child_operation(index: int) -> None:
    with progress_display("verbose") as display, display.bar(f"Child {index}", total=3) as task:
        for _ in task.track(range(3)):
            time.sleep(0.1)


def test_child_processes_forward_progress(console_output: Callable[[], str]) -> None:
    """Test if progress from attached child processes is shown in the parent display."""
    with (
        progress_display("transient") as display,
        ProcessPoolExecutor(
            max_workers=2,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=display.worker_initializer(),
        ) as executor,
    ):
        list(executor.map(_run_child_operation, range(2)))

    output = console_output()
    assert re.search(r"Child 0\s+100%.*3/3", output)
    assert re.search(r"Child 1\s+100%.*3/3", output)
    assert rich_utils._SHARED_LIVE is None


def _group_type_in_child() -> str:
    with progress_display("verbose") as display:
        return type(display._group).__name__


@pytest.mark.skipif(sys.platform == "win32", reason="Fork doesn't exist on Windows")  # type: ignore
def test_forked_child_process_does_not_use_inherited_display(
    console_output: Callable[[], str],
) -> None:
    """Test if a forked child process ignores the display open in its parent."""
    with (
        progress_display("verbose") as display,
        ProcessPoolExecutor(
            max_workers=1, mp_context=multiprocessing.get_context("fork")
        ) as executor,
    ):
        assert not display.is_silent
        assert executor.submit(_group_type_in_child).result() == "_PlainGroup"


def _run_operation_in_mode(mode: VERBOSITY_MODE) -> None:
    with progress_display(mode, show_elapsed_time=True) as display, display.spinner("Child step"):
        pass


@pytest.mark.parametrize(  # type: ignore
    "mode,expected_output",
    [
        ("verbose", r"pid \d+: Child step • 0:00:00\npid \d+: Finished operation in 0:00:0\d\n"),
        ("transient", r"pid \d+: Finished operation in 0:00:0\d\n"),
        ("silent", r""),
    ],
)
def test_child_processes_without_initializer_print_plain_lines(
    mode: VERBOSITY_MODE,
    expected_output: str,
    console_output: Callable[[], str],
    capfd: pytest.CaptureFixture[str],
) -> None:
    """Test if child processes not attached to the parent print lines without a live display."""
    with ProcessPoolExecutor(
        max_workers=1, mp_context=multiprocessing.get_context("spawn")
    ) as executor:
        executor.submit(_run_operation_in_mode, mode).result()

    assert re.fullmatch(expected_output, capfd.readouterr().out.replace("\r\n", "\n"))
    assert console_output() == ""
