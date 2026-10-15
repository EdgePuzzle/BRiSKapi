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
    raise RuntimeError(f'{NAME} is not installed. briskapi\'s wheels hold it for Linux, macOS and Windows on 64-bit Intel and Arm; '
                       'on another platform build it (cargo build --release in rust/brisk_parquet of '
                       'https://github.com/honvl/BRiSKapi) and set BRISK_PARQUET to the program')


def run(arguments, progress=None, lines=None) -> str:
    """Run `brisk-parquet` and return what it prints. `progress(label)` is told of each `progress: label` line as it
    comes; `lines` (an iterable of bytes) is what it reads. A recording it refuses raises ValueError with its message; any
    other failure, RuntimeError."""
    process = subprocess.Popen([binary(), *map(str, arguments)], stdin=subprocess.PIPE if lines is not None else None,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=lines is None)
    messages = []

    def watch():
        for line in process.stderr:
            line = line.decode(errors='replace') if isinstance(line, bytes) else line
            line = line.rstrip('\n')
            if line.startswith('progress: '):
                if progress:
                    progress(line[len('progress: '):])
            else:
                messages.append(line)

    def feed():
        try:
            for line in lines:
                process.stdin.write(line if line.endswith(b'\n') else line + b'\n')
        except BrokenPipeError:
            pass   # it stopped reading: its exit says why
        finally:
            try:
                process.stdin.close()
            except BrokenPipeError:
                pass
    readers = [threading.Thread(target=watch, daemon=True)]
    if lines is not None:
        readers.append(threading.Thread(target=feed, daemon=True))
    for reader in readers:
        reader.start()
    out = process.stdout.read()
    process.wait()
    for reader in readers:
        reader.join()
    out = out.decode() if isinstance(out, bytes) else out
    if process.returncode == 0:
        return out
    message = '\n'.join(messages)
    if process.returncode == 2 and message.startswith('refused: '):
        raise ValueError(message[len('refused: '):])
    raise RuntimeError(message or f'{NAME} failed ({process.returncode})')


def part(lines, out, midnight, threads=None):
    """Write the decoder's JSON lines (bytes, each with its newline) as a spool part in the folder `out`: the depths, trades
    and batch tables, rows sorted by issue and batch. `midnight` is the day's start in UTC nanoseconds. Returns the counts."""
    arguments = ['part', '--out', out, '--midnight', midnight]
    if threads:
        arguments += ['--threads', threads]
    return json.loads(run(arguments, lines=lines))


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
