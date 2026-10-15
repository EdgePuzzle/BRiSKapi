"""The Rust tool for the slow parts of the recordings (rust/brisk_parquet): `brisk-parquet`, run as a program.

The archive's function and the recorder use it where Python took minutes: checking a broker's day and packaging its
pieces. It reads and writes the same tables `briskapi._parquet` defines.
"""
import json
import os
import shutil
import subprocess
import threading
from pathlib import Path

NAME = 'brisk-parquet'
_HERE = Path(__file__).resolve().parent


def binary() -> str:
    """The program: BRISK_PARQUET if set, else the one shipped with briskapi, else a build in the source tree, else on the PATH."""
    candidates = [os.environ.get('BRISK_PARQUET'), _HERE / '_bin' / NAME, _HERE.parent / 'rust' / 'brisk_parquet' / 'target' / 'release' / NAME]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return str(candidate)
    found = shutil.which(NAME)
    if found:
        return found
    raise RuntimeError(f'{NAME} is not installed: reinstall briskapi, or build it with cargo build --release --manifest-path rust/brisk_parquet/Cargo.toml')


def run(arguments, progress=None) -> str:
    """Run `brisk-parquet` and return what it prints. `progress(label)` is told of each `progress: label` line as it
    comes. A recording it refuses raises ValueError with its message; any other failure, RuntimeError."""
    process = subprocess.Popen([binary(), *map(str, arguments)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    lines = []

    def watch():
        for line in process.stderr:
            line = line.rstrip('\n')
            if line.startswith('progress: '):
                if progress:
                    progress(line[len('progress: '):])
            else:
                lines.append(line)
    reader = threading.Thread(target=watch, daemon=True)
    reader.start()
    out = process.stdout.read()
    process.wait()
    reader.join()
    if process.returncode == 0:
        return out
    message = '\n'.join(lines)
    if process.returncode == 2 and message.startswith('refused: '):
        raise ValueError(message[len('refused: '):])
    raise RuntimeError(message or f'{NAME} failed ({process.returncode})')


def package(spool, parts, out, midnight, *, day_start=None, cut=None, end='parts', state_out=None, threads=None, range_rows=None):
    """Write consecutive parts of a spooled segment (`spool`: its folder) as a recording in `out`, a range of issues at a
    time. After a piece, `cut` is (the file `state_out` gave then, the clock it ended at) and `day_start` the day's first
    part. `end`: 'parts' for the parts' own, 'cut' for an end to a piece that stops early, 'repair' for one if they have none.
    Returns {'summary', 'clock' (the last batch's), 'repaired'}; `state_out` gets each issue's last quote."""
    arguments = ['package', '--spool', spool, '--midnight', midnight, '--out', out, '--end', end]
    for flag, value in (('--day-start', day_start), ('--state-out', state_out), ('--threads', threads), ('--range-rows', range_rows)):
        if value is not None:
            arguments += [flag, value]
    if cut:
        arguments += ['--cut-state', cut[0], '--cut-clock', cut[1]]
    return json.loads(run([*arguments, *parts]))


def state(spool, parts, midnight, state_out, *, day_start=None, cut=None, threads=None):
    """Each issue's last quote after `parts` (and after `cut`'s book, if they follow a piece), written to `state_out`, and the
    last batch's clock."""
    arguments = ['state', '--spool', spool, '--midnight', midnight, '--state-out', state_out]
    for flag, value in (('--day-start', day_start), ('--threads', threads)):
        if value is not None:
            arguments += [flag, value]
    if cut:
        arguments += ['--cut-state', cut[0], '--cut-clock', cut[1]]
    return json.loads(run([*arguments, *parts]))['clock']


def threads() -> int:
    """How many threads packaging a day may use: a third of the cores, between 2 and 6. The live feed and its decoder host run
    meanwhile (the afternoon's, as the morning is packaged), and come first."""
    return min(6, max(2, (os.cpu_count() or 1) // 3))
