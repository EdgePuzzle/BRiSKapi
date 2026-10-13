"""A recording kept on disk while it is made, so that an interruption doesn't lose it.

A feed writes each segment (a bootstrap to its end: a trading day, or the part of one before a lost
connection) to a spool folder as its batches arrive, in the recording's own Parquet tables, a part at a time:

    spool/segment-ID/owner.json                  the process writing it
    spool/segment-ID/data/equity.parquet         the segment's master, written with its bootstrap
    spool/segment-ID/parts/000001/data/...       the depths, trades and batch tables of each closed part

A part closes every PART_SECONDS, or sooner after PART_BYTES of decoder output, and when the segment ends or
the feed stops, so a crash loses at most the part still open. A segment takes about the disk of its finished
recording (its decoded JSON would take some twenty times more). Once it ends, package() turns the parts into
the canonical recording: the same bytes as from the segment's batches at once. A feed that crashed leaves its
folder behind; the next feed keeps what it holds as a recording on this computer (recover()).
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
import warnings

from briskapi import _parquet
from briskapi.schema import SKIPPED, canonical_batches, local_name

PART_SECONDS = 30
PART_BYTES = 64 * 2**20
DEMO_SOURCES = ('historical_mock', 'synthetic_test')   # the same for everyone: nothing to keep


def data_dir() -> Path:
    """Where briskapi keeps data on this computer (XDG_DATA_HOME, else ~/.local/share)."""
    return Path(os.environ.get('XDG_DATA_HOME') or Path.home() / '.local' / 'share') / 'brisk'


def spool_root() -> Path:
    return data_dir() / 'spool'


def kept_dir() -> Path:
    """Where a stopped or recovered broker segment is kept when its feed has no `output`."""
    return data_dir() / 'recordings'


def _fsync(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Spool:
    """One segment's spool. The feed calls add() with each batch and its line, in order, from one thread."""

    def __init__(self, root=None):
        root = Path(root or spool_root())
        root.mkdir(parents=True, exist_ok=True)
        self.path = Path(tempfile.mkdtemp(prefix='segment-', dir=root))
        (self.path / 'owner.json').write_text(json.dumps({'pid': os.getpid(), 'started': time.time()}))
        self.header = None        # the bootstrap without quotes and trades
        self.master = None
        self.ended = False
        self.count = 0            # lines so far
        self._lock = threading.Lock()
        self._lines, self._size, self._opened = [], 0, time.monotonic()
        self._pending = []        # [(first line, lines)] handed to the writer, not yet closed
        self._parts = []          # [(folder, first line, end line)] closed
        self._carry = {}          # trade IDs across parts
        self._writer = ThreadPoolExecutor(1, thread_name_prefix='brisk-spool')
        self._error = None
        self._readers, self._removed = 0, False

    def __repr__(self):
        return f'Spool({str(self.path)!r}, lines={self.count})'

    def add(self, batch, line: bytes):
        if self._error:
            raise self._error
        if batch['type'] == 'bootstrap':
            # As a recording keeps it: fixed transport provenance only, never connection diagnostics.
            batch = next(canonical_batches(io.BytesIO(line)))
            self.header = {k: v for k, v in batch.items() if k != SKIPPED} | {'master': [], 'quotes': [], 'trades': []}
            self.master = batch['master']
            equity = self.path / _parquet.EQUITY
            equity.parent.mkdir(parents=True, exist_ok=True)
            _parquet.pq.write_table(_parquet.equity_table(batch), equity, **_parquet.WRITE)
            _fsync(equity)
        with self._lock:
            self._lines.append(line)
            self._size += len(line)
            self.count += 1
            self.ended = batch['type'] == 'end'
        if self.ended or self._size >= PART_BYTES or time.monotonic() - self._opened >= PART_SECONDS:
            self.close_part()

    def close_part(self, wait=False):
        """Hand the open part to the writer (and with `wait`, until every part is on disk)."""
        with self._lock:
            lines, first = self._lines, self.count - len(self._lines)
            self._lines, self._size, self._opened = [], 0, time.monotonic()
            if lines:
                self._pending.append((first, lines))
                number = len(self._parts) + len(self._pending)
        if lines:
            self._writer.submit(self._write, number, first, lines)
        if wait:
            self._writer.submit(lambda: None).result()
            if self._error:
                raise self._error

    def _write(self, number, first, lines):
        if self._error:
            return
        try:
            folder = self.path / 'parts' / f'{number:06d}'
            staging = folder.with_name(f'.{folder.name}')
            shutil.rmtree(staging, ignore_errors=True)
            # Canonical, as package() makes them: nothing of the connection's diagnostics reaches the disk.
            batches = list(canonical_batches(io.BytesIO(b''.join(lines))))
            _parquet.write_part(batches, self.header, staging, self._carry)
            for name in (_parquet.BATCHES, _parquet.DEPTHS, _parquet.TRADES):
                _fsync(staging / name)
            os.replace(staging, folder)
            with self._lock:
                self._pending.pop(0)
                self._parts.append((folder, first, first + len(lines)))
        except Exception as error:  # noqa: BLE001 - the feed raises it with its next batch
            self._error = error

    def replay(self, count):
        """The segment's first `count` lines, as a Live replay sends them: closed parts re-encoded, then the lines
        not yet written as they arrived. The spool stays until the replay is read or closed."""
        with self._lock:
            parts, pending = list(self._parts), [(first, list(lines)) for first, lines in self._pending]
            pending.append((self.count - len(self._lines), list(self._lines)))
            self._readers += 1
        return _Replay(self, parts, pending, count)

    def _release(self):
        with self._lock:
            self._readers -= 1
            gone = self._removed and not self._readers
        if gone:
            shutil.rmtree(self.path, ignore_errors=True)

    def parts(self):
        return [folder for folder, _, _ in self._parts]

    def package(self, output):
        """The segment, which ended cleanly, as a package in `output` (see briskapi.cli.package_parts)."""
        from briskapi import cli
        self.close_part(wait=True)
        return cli.package_parts(self.path / _parquet.EQUITY, self.parts(), Path(output))

    def keep(self, root):
        """The segment so far, which did not end cleanly, as a recording (tables, no manifest) in a folder of `root`
        named after it; None when nothing reached the disk."""
        self.close_part(wait=True)
        return keep_parts(self.path / _parquet.EQUITY, self.parts(), root)

    def remove(self):
        """Delete the spool (once any replay still reading it ends)."""
        self._writer.shutdown(wait=True)
        with self._lock:
            self._removed = True
            readers = self._readers
        if not readers:
            shutil.rmtree(self.path, ignore_errors=True)


class _Replay:
    def __init__(self, spool, parts, pending, count):
        self._spool, self._parts, self._pending, self._count, self._open = spool, parts, pending, count, True

    def __iter__(self):
        try:
            at = 0
            for folder, _, _ in self._parts:
                for batch in _parquet.part_batches(folder, self._spool.header, self._spool.master):
                    if at >= self._count:
                        return
                    yield json.dumps(batch, separators=(',', ':'), ensure_ascii=False).encode() + b'\n'
                    at += 1
            for first, lines in self._pending:
                for line in lines[max(at - first, 0):]:
                    if at >= self._count:
                        return
                    yield line
                    at += 1
        finally:
            self.close()

    def close(self):
        if self._open:
            self._open = False
            self._spool._release()


def _alive(pid) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def recover(output=None, root=None):
    """Keep what feeds that crashed left in the spool: each broker segment as a recording in `output` (else in
    kept_dir()), never uploaded; a demo's is the same for everyone and is dropped. Returns the folders kept."""
    root, kept = Path(root or spool_root()), []
    for folder in sorted(root.glob('segment-*')) if root.is_dir() else ():
        try:
            owner = json.loads((folder / 'owner.json').read_text())
        except (OSError, ValueError):
            owner = {}
        if isinstance(owner.get('pid'), int) and owner['pid'] != os.getpid() and _alive(owner['pid']):
            continue
        parts = sorted(p for p in (folder / 'parts').glob('[0-9]*') if p.is_dir()) if (folder / 'parts').is_dir() else []
        try:
            source = parts and _parquet.pq.read_table(parts[0] / _parquet.BATCHES, columns=['source']).column(0)[0].as_py()
            if source and source not in DEMO_SOURCES:
                target = keep_parts(folder / _parquet.EQUITY, parts, output or kept_dir())
                if target:
                    kept.append(target)
        except Exception as error:  # noqa: BLE001 - a damaged spool must not stop the next feed
            warnings.warn(f'Could not recover the recording in {folder}: {error}')
            continue
        shutil.rmtree(folder, ignore_errors=True)
    return kept


def keep_parts(equity, parts, root):
    """Write spooled parts as a recording (tables, no manifest) in a new folder of `root` named after the
    segment, SOURCE_YYYYMMDD_FROM-TO; None when there are none."""
    if not parts:
        return None
    columns = _parquet.load_parts(equity, parts)
    first, last = columns.batches[0], columns.batches[-1]
    name = local_name({'source': first['source'], 'trading_date': first['trading_date'],
                       'first_source_time_us': first['source_time_us'], 'last_source_time_us': last['source_time_us']})
    target = Path(root) / name
    for n in range(2, 100):
        if not target.exists():
            break
        target = Path(root) / f'{name}-{n}'
    columns.write(target)
    return target
