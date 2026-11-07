# Progress display

`rq_geo_toolkit.rich_utils` renders the progress of a whole operation in one live display.
Libraries built on it declare their steps and open spinners or bars. The module takes care of
terminals, Jupyter notebooks, threads, child processes and verbosity modes.

## Quick start

Decorate the public function and open rows inside it:

```python
from rq_geo_toolkit.rich_utils import progress_display, with_progress_display


@with_progress_display(total_steps=2)
def convert(path, verbosity_mode="transient"):
    with progress_display() as display:
        with display.spinner("Reading file", step="major"):
            rows = read(path)

        with display.bar("Saving rows", step="major") as bar:
            for row in bar.track(rows):
                save(row)
```

The output in the `verbose` mode:

```text
  [1/2] Reading file • 0:00:01
  [2/2] Saving rows 100% ━━━━━━━━━━ 500/500 • 0:00:04 < 0:00:00 • 125.00 it/s
Finished operation in 0:00:05
```

## Rules

- Only the outermost `progress_display` opens a display. Every nested call returns the active
  one and ignores its `verbosity_mode`, so calling it in every helper is cheap and safe.
- Don't create your own `rich.progress.Progress` or `rich.live.Live` inside an operation. Rich
  allows one live display per console.
- Scripts starting child processes need the `if __name__ == "__main__":` guard, like every
  script using `multiprocessing` with the `spawn` start method.

## Verbosity modes

| Mode        | Behaviour                                                      |
| ----------- | -------------------------------------------------------------- |
| `silent`    | Discards all progress. Warnings are still written to stderr.   |
| `transient` | Shows running rows and removes them when they finish.          |
| `verbose`   | Keeps finished rows in the output.                             |

`with_progress_display` reads the mode from the `verbosity_mode` argument of the call. For methods
without this argument it reads `self.verbosity_mode`. The default is `transient`.

## Decorator

`with_progress_display` opens a display for the whole call and prints the total elapsed time.
The time isn't printed when the operation failed or didn't show any row, for example after
finding a cached result.

Use it directly, or with arguments when the operation has numbered steps:

```python
@with_progress_display
def simple_operation(verbosity_mode="transient"): ...


@with_progress_display(total_steps=4)
def operation_with_steps(verbosity_mode="transient"): ...
```

Use `progress_display` directly when a function needs a display without the elapsed time.

## Rows

```python
with progress_display(verbosity_mode) as display:
    with display.spinner("Waiting for the query"):
        run_query()

    with display.bar("Processing", total=len(files)) as bar:
        for file in files:
            process(file)
            bar.update(advance=1)

    display.print("A permanent line with [bold]rich markup[/bold]")
```

`ProgressTask`, returned by `display.bar`, has these methods:

| Method                                                  | Use                                       |
| ------------------------------------------------------- | ----------------------------------------- |
| `update(completed, total=..., advance=..., description=...)` | Change the state of the row. The total can be set after opening the row. |
| `track(iterable, total=None)`                           | Advance after each value. The total defaults to `len(iterable)`. |
| `print_progress()`                                      | Print the current state as a permanent line. |

A bar that ends with a total of 0 is shown as `skipped`.

Warnings raised during an operation are printed above the rows, so they don't break the display.
Filters and `pytest.warns` keep working.

## Numbered steps

Pass `total_steps` to number the rows. `step="major"` counts the next step of the operation and
`step="minor"` counts the next step inside a step group. Rows without `step` aren't numbered.

```python
@with_progress_display(total_steps=3)
def operation(verbosity_mode="transient"):
    with progress_display() as display:
        with display.spinner("Reading", step="major"):
            ...

        display.step_group("Filtering", 2)
        with display.spinner("Selecting rows", step="minor"):
            ...
        with display.spinner("Removing duplicates", step="minor"):
            ...

        if has_output:
            with display.spinner("Saving", step="major"):
                ...
        else:
            display.skip_step("Saving")
```

```text
  [1/3] Reading • 0:00:01
  [2/3] Filtering
   ├─ [1/2] Selecting rows • 0:00:02
   └─ [2/2] Removing duplicates • 0:00:01
  [3/3] Saving • skipped
```

The totals are declared by hand, so keep them correct:

- Every conditional step needs a `skip_step` in the other branch.
- `step_group` counts as one major step. Its second argument is the number of its minor steps.
- A group stays open until the next major step or the end of the operation.
- Use `display.steps.add_steps(count)` when the number of steps is known only during
  the operation, for example after checking which files are already cached.

### Nested operations

Decorated operations can call each other, and each one declares only its own steps:

- An operation started before the first step of the outer one continues its numbering.
- An operation started later runs inside the open step group. Its major steps are counted as
  minor steps and its total extends the total of the group.
- Without an open group, a new one called `Nested operation` is created. Call `step_group` before
  the nested operation to give it a proper name.
- `step_group` called in a nested operation doesn't add a row. It replaces the single step it was
  counted as with `total` minor steps.

### Retries

A retried part of an operation counts its steps again, so rewind the counter before the retry:

```python
first_step = display.steps.major_step_number
while True:
    try:
        run_steps()
        break
    except MemoryError:
        lower_memory_usage()
        display.steps.major_step_number = first_step
```

In the `verbose` mode the repeated steps are printed with the same numbers again. A spinner or a bar that ends with an `Exception` is shown as `failed`, so the failed attempt can be told apart from the repeated one. Interruptions outside of `Exception`, like `KeyboardInterrupt`, `SystemExit` or `asyncio.CancelledError`, don't mark it.

### Files processed one by one

`StepCounter.reset` starts the numbering again with a prefix, like `[1/3][ 2/16] Reading`:

```python
display.steps.reset(prefix=f"[{file_number}/{total_files}]")
```

## Downloads

`display.pooch_progressbar` returns an object for the `progressbar` argument of `pooch`.
It keeps the transfer speed correct after `pooch` refills the bar at the end of a download.
It's falsy in the `silent` mode, which disables the bar in `pooch`.

```python
with progress_display(verbosity_mode) as display:
    pooch.retrieve(
        url,
        known_hash=None,
        progressbar=display.pooch_progressbar("Downloading file"),
    )
```

## Threads and processes

Each thread running an operation gets its own group of rows in the shared display. When groups
overlap, rows and printed lines are prefixed with `#1`, `#2` and so on. Nothing needs to be done
to enable this. A thread started inside an operation doesn't reuse its display.

Child processes can't draw in the display of the parent:

- By default they print plain lines prefixed with `pid N:`. Rows are printed only in the `verbose`
  mode, and the `silent` mode stays silent.
- To show their rows in the parent display, pass `display.worker_initializer()` as the
  `initializer` of a pool. The pool must not outlive the display.

```python
with progress_display(verbosity_mode) as display:
    with ProcessPoolExecutor(initializer=display.worker_initializer()) as executor:
        list(executor.map(work, items))
```

Another live display that is already running, for example one started by the user, makes
operations fall back to the same plain lines.

## Jupyter

Finished rows of the `verbose` mode stay in the live widget, because every printed line is
a separate cell output, which leaves gaps between rows. This requires `ipywidgets`. Without it,
finished rows are printed like in a terminal.

## Environment variables

`FORCE_TERMINAL_MODE=true` forces terminal output without interactive features. CI jobs and
documentation builds use it.

## Testing

`tests/test_rich_utils.py` covers this module. Libraries with hand-counted steps should also check
in their test suite that operations run exactly the declared number of major and minor steps.
