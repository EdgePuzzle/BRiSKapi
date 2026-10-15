"""The Rust tool for the slow parts of the recordings (rust/brisk_parquet): `brisk-parquet`, run as a program.

The archive's function and the recorder use it where Python took minutes: checking a broker's day and packaging its
pieces. It reads and writes the same tables `briskapi._parquet` defines.
"""
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
