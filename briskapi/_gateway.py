"""briskapi.Live: Databento-style live records from the feed running on this computer.

    live = briskapi.Live()                     # no key or sign-in: the local feed's own
    live.subscribe(dataset="XTKS.BRISK", schema="imbalance", symbols=["7203"])
    for record in live:
        print(record.symbol, record.ind_match_price)

A feed started with a gateway (`brisk live` does by default; briskapi.connect(gateway=True)) serves its
decoded batches to Live clients on this computer only. It listens on 127.0.0.1 and admits a client only
with the key it writes, with its port, to an owner-only file (~/.config/brisk/gateway/DATASET.json), which
Live reads. Each client turns the batches into the records the cloud API returns (briskapi.records), so
a record is the same live as in history. Only the feed signs in to the broker.

The protocol (newline-delimited JSON, version 1): the client sends {"version", "key", "dataset", "symbols",
"replay", "snapshot"}; the gateway answers {"gateway": "session", ...} with the trading day's bootstrap
(its master, without quotes) and, if asked, each security's current quote; then, if asked and the day
is running, the day so far exactly as the decoder wrote it; then {"gateway": "live"}; then each batch as
it arrives (only the asked symbols' master entries, quotes and trades), and last {"gateway": "closed"}.
A client that falls more than MAX_BEHIND bytes behind the feed is sent {"gateway": "error"} and dropped,
so a slow reader never slows the feed or its recording.
"""
from __future__ import annotations

from collections import deque
import hmac
import io
import json
import os
from pathlib import Path
import queue
import secrets
import socket
import threading
from typing import Callable, NamedTuple
import warnings

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from briskapi import _parquet, records
from briskapi._recording import BriskError
from briskapi.sites import config_dir

VERSION = 1
HOST = '127.0.0.1'
MAX_BEHIND = 1024**3
MAX_CLIENTS = 32
MAX_REQUEST = 1 << 20
_LIVE = b'{"gateway":"live"}\n'
_END = object()


def gateway_path(dataset) -> Path:
    """The owner-only file where a feed serving `dataset` leaves its port and key."""
    return config_dir() / 'gateway' / f'{dataset}.json'


def _line(value) -> bytes:
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode() + b'\n'


def _read_line(stream, limit=None):
    line = stream.readline() if limit is None else stream.readline(limit)
    if not line.endswith(b'\n'):
        raise ConnectionError('The connection ended')
    return json.loads(line)


def _read_file(path):
    try:
        info = json.loads(Path(path).read_text())
        return info if isinstance(info, dict) and isinstance(info.get('port'), int) and isinstance(info.get('key'), str) else None
    except (OSError, ValueError):
        return None


def _answers(info) -> bool:
    """Whether a gateway still answers at the port a file names (a crashed feed leaves its file behind)."""
    try:
        with socket.create_connection((HOST, info['port']), timeout=2) as sock:
            sock.sendall(_line({'version': VERSION, 'key': info['key'], 'probe': True}))
            return _read_line(sock.makefile('rb'), MAX_REQUEST).get('gateway') == 'probe'
    except (OSError, ValueError):
        return False


# The gateway, in the feed's process

class Gateway:
    """Serves one feed's batches to Live clients on this computer (see the module docstring). The feed calls
    publish() with each batch in order on its own thread, and close() when it ends."""

    def __init__(self, dataset):
        self.dataset, self.key = dataset, secrets.token_urlsafe(32)
        self._server = socket.create_server((HOST, 0))
        self._server.settimeout(0.25)
        self.port = self._server.getsockname()[1]
        self._lock = threading.Lock()
        self._clients: list[_Client] = []
        self._bootstrap = None   # the trading day's bootstrap batch, without quotes and trades
        self._quotes = {}        # issue_id -> its latest quote
        self._day = None         # [the feed's file of the day's batches, its length so far] while the day runs
        self._closed = None      # the last line, once the feed has ended
        self.path = gateway_path(dataset)
        current = _read_file(self.path)
        if current is not None and _answers(current):
            self._server.close()
            raise BriskError(f'Another feed on this computer already serves {dataset} to briskapi.Live '
                             f'(process {current.get("pid")}); this one does not')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        staging = self.path.with_name(f'.{self.path.name}.{secrets.token_hex(4)}')
        with os.fdopen(os.open(staging, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as f:
            json.dump({'version': VERSION, 'dataset': dataset, 'port': self.port, 'key': self.key, 'pid': os.getpid()}, f)
        os.replace(staging, self.path)
        threading.Thread(target=self._accept, name='brisk-gateway', daemon=True).start()

    def __repr__(self):
        return f'Gateway({self.dataset!r}, {HOST}:{self.port})'

    def publish(self, batch, line, day_file=None):
        """Pass on a batch and its line as the decoder wrote it, which `day_file` (if any) now ends with."""
        with self._lock:
            if self._closed:
                return
            if batch['type'] == 'bootstrap':
                self._bootstrap = {k: v for k, v in batch.items() if k not in ('quotes', 'trades')}
                self._quotes, self._day = {}, [day_file, 0] if day_file else None
            if self._day:
                self._day[1] += len(line)
            for quote in batch.get('quotes', ()):
                self._quotes[quote['issue_id']] = quote
            if batch['type'] == 'end':
                self._day = None   # the feed packages and removes the day's file
            for client in self._clients:
                client.send(batch, line)

    def close(self, status='closed', error=None):
        """End every session with the feed's status and stop serving."""
        with self._lock:
            if self._closed:
                return
            self._closed = _line({'gateway': 'closed', 'status': status, 'error': None if error is None else str(error)})
            for client in self._clients:
                client.put(self._closed, last=True)
        current = _read_file(self.path)
        if current and hmac.compare_digest(current['key'], self.key):
            self.path.unlink(missing_ok=True)

    def _accept(self):
        while not self._closed:
            try:
                sock, _ = self._server.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            threading.Thread(target=self._serve, args=(sock,), name='brisk-gateway-client', daemon=True).start()
        self._server.close()

    def _serve(self, sock):
        client = replay = None
        try:
            sock.settimeout(10)
            try:
                request = _read_line(sock.makefile('rb'), MAX_REQUEST)
            except ValueError:
                request = None
            sock.settimeout(None)
            refused = self._refuse(request)
            if refused:
                return sock.sendall(_line({'gateway': 'error', 'error': refused}))
            if request.get('probe'):
                return sock.sendall(_line({'gateway': 'probe', 'dataset': self.dataset}))
            try:
                codes = records.symbols_of(request.get('symbols'))
            except (TypeError, ValueError) as error:
                return sock.sendall(_line({'gateway': 'error', 'error': str(error)}))
            with self._lock:
                if self._closed:
                    return sock.sendall(self._closed)
                if len(self._clients) >= MAX_CLIENTS:
                    return sock.sendall(_line({'gateway': 'error', 'error': f'At most {MAX_CLIENTS} Live sessions at once'}))
                # What the client is told, and the batches it is sent after, are taken under the lock that
                # publish() holds: no batch is missed or sent twice.
                bootstrap, quotes = self._bootstrap, list(self._quotes.values()) if request.get('snapshot') else None
                if request.get('replay') and self._day:
                    replay = (open(self._day[0], 'rb'), self._day[1])
                client = _Client(sock, codes)
                self._clients.append(client)
            if bootstrap is not None and codes is not None:
                bootstrap = dict(bootstrap, master=[m for m in bootstrap['master'] if m['code'] in codes])
            snapshot = None if quotes is None or bootstrap is None else \
                sorted((q for q in quotes if codes is None or q['code'] in codes), key=lambda q: q['issue_id'])
            client.run(_line({'gateway': 'session', 'version': VERSION, 'dataset': self.dataset, 'bootstrap': bootstrap,
                              'snapshot': snapshot, 'replay': replay is not None}), replay)
        except (OSError, ValueError, ConnectionError):
            pass
        finally:
            if client:
                with self._lock:
                    self._clients.remove(client)
            if replay:
                replay[0].close()
            sock.close()

    def _refuse(self, request):
        if not isinstance(request, dict) or request.get('version') != VERSION:
            return f'This gateway speaks version {VERSION}; update briskapi'
        if not isinstance(request.get('key'), str) or not hmac.compare_digest(request['key'].encode(), self.key.encode()):
            return 'Wrong key'
        if not request.get('probe') and request.get('dataset') != self.dataset:
            return f'This gateway serves {self.dataset}'
        return None


def _filtered(batch, codes):
    """A batch's line with only these codes' master entries, quotes and trades; None when nothing is left."""
    out = dict(batch)
    for name in ('master', 'quotes', 'trades'):
        if name in batch:
            out[name] = [item for item in batch[name] if item['code'] in codes]
    if batch['type'] in ('quotes', 'history') and not out.get('quotes') and not out.get('trades'):
        return None
    return _line(out)


class _Client:
    """One session's outbox: what publish() hands it is sent on the session's own thread."""

    def __init__(self, sock, codes):
        self.sock, self.codes = sock, codes
        self._items, self._behind, self._last = deque(), 0, False
        self._ready = threading.Condition()

    def send(self, batch, line):
        data = line if self.codes is None else _filtered(batch, self.codes)
        if data:
            self.put(data)

    def put(self, data, last=False):
        with self._ready:
            if self._last:
                return
            if self._behind + len(data) > MAX_BEHIND and not last:
                # A slow reader is dropped, never waited for: the feed and its recording go on.
                self._items.clear()
                self._behind = 0
                data, last = _line({'gateway': 'error', 'error': 'This session fell more than '
                                    f'{MAX_BEHIND // 1024**2} MB behind the feed and was dropped'}), True
                threading.Timer(10, self._stop).start()
            self._items.append(data)
            self._behind += len(data)
            self._last = last
            self._ready.notify()

    def _stop(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def run(self, header, replay):
        self.sock.sendall(header)
        if replay:
            self.sock.sendfile(replay[0], 0, replay[1])
        self.sock.sendall(_LIVE)
        while True:
            with self._ready:
                self._ready.wait_for(lambda: self._items)
                data = self._items.popleft()
                self._behind -= len(data)
                last = self._last and not self._items
            self.sock.sendall(data)
            if last:
                return


# The client

class Record(dict):
    """A record: a dict of its fields (as Historical's records), also readable as attributes (record.price),
    with its schema in `record.schema`."""

    __slots__ = ('schema',)

    def __init__(self, fields, schema):
        super().__init__(fields)
        self.schema = schema

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    def __repr__(self):
        return f'Record({self.schema!r}, {dict.__repr__(self)})'


class _Subscription(NamedTuple):
    schema: str
    symbols: set | None
    start: int | None
    snapshot: bool


class _Day:
    """What projecting a trading day's batches needs: its bootstrap without market data, its equity table,
    and the trade IDs' places so far."""

    def __init__(self, bootstrap):
        self.header = {**bootstrap, 'master': [], 'quotes': [], 'trades': []}
        self.equity = _parquet.equity_table(bootstrap)
        self.midnight = _parquet.midnight_ns(bootstrap['trading_date'])
        self.carry = {}


class Live:
    """Databento-style live records from the feed on this computer (see the module docstring).

    Names follow databento.Live: subscribe() before start(), then iterate, or add_callback() / add_stream()
    and block_for_close(). Records are briskapi.records' schemas, as Historical returns them, in the order
    the feed receives them (within a batch, as Historical orders them). `start` replays the feed's trading
    day from that time before going live (as far back as the feed has it: from its start, and the day's
    trades its snapshot restored); `snapshot=True` begins with each security's current quote.
    """

    def __init__(self, key: str | None = None, gateway: str = HOST, port: int | None = None):
        self._key, self._host, self._port = key, gateway, port
        self.dataset = None
        self.symbology_map: dict[int, str] = {}
        self._subscriptions: list[_Subscription] = []
        self._callbacks: list = []
        self._streams: list = []
        self._queue = None
        self._sock = None
        self._thread = None
        self._error = None
        self._day = None
        self._stopping = False
        self._done = threading.Event()

    def __repr__(self):
        state = 'closed' if self._done.is_set() else 'streaming' if self._thread else 'ready'
        return f'Live(dataset={self.dataset!r}, {state})'

    def subscribe(self, dataset, schema, symbols='ALL_SYMBOLS', stype_in='raw_symbol', start=None, snapshot=False):
        """Ask for `schema`'s records of `symbols` (JPX codes). `start` (a time, a date or UTC ns; 0 for all the
        feed has) replays from then; `snapshot` starts mbp-10, imbalance or brisk-quotes with the current quotes."""
        if self._thread:
            raise BriskError('Subscribe before start(): a session streams the subscriptions it started with')
        if dataset not in records.DATASETS:
            raise ValueError(f'Unknown dataset {dataset!r}; one of {", ".join(records.DATASETS)}')
        if self.dataset not in (None, dataset):
            raise ValueError(f'This session streams {self.dataset}; use another Live for {dataset}')
        if schema in records.BARS:
            raise ValueError('OHLCV bars come only from the cloud API (briskapi.Historical)')
        if schema not in records.LIVE_SCHEMAS:
            raise ValueError(f'Unknown schema {schema!r}; one of {", ".join(records.LIVE_SCHEMAS)}')
        if stype_in not in (None, '', 'raw_symbol'):
            raise ValueError('Only stype_in=raw_symbol (JPX codes) is supported')
        if snapshot and start is not None:
            raise ValueError('Choose start (a replay) or snapshot (the current quotes), not both')
        if snapshot and schema not in ('mbp-10', 'imbalance', 'brisk-quotes'):
            raise ValueError('snapshot applies to mbp-10, imbalance and brisk-quotes')
        info = None if self._key and self._port else _read_file(gateway_path(dataset))
        if info is None and not (self._key and self._port):
            how = '`brisk live --sbi` (or --site e-shiten)' if dataset == 'XTKS.BRISK' else '`brisk live --web`'
            raise BriskError(f'No feed on this computer serves {dataset}: start one with {how}, '
                             'or briskapi.connect(..., gateway=True)')
        self._key, self._port = self._key or info['key'], self._port or info['port']
        self.dataset = dataset
        self._subscriptions.append(_Subscription(schema, records.symbols_of(symbols),
                                                 None if start is None else records.timestamp(start), bool(snapshot)))

    def add_callback(self, record_callback: Callable, exception_callback: Callable | None = None):
        """Call `record_callback(record)` for each record, on the session's thread; an exception it raises goes to
        `exception_callback`, or is warned about, and the session goes on."""
        self._callbacks.append((record_callback, exception_callback))

    def add_stream(self, stream, exception_callback: Callable | None = None):
        """Write each record to `stream` (a path, created anew, or a file) as one JSON line (Databento writes DBN)."""
        owned = isinstance(stream, (str, os.PathLike))
        self._streams.append((open(stream, 'xb') if owned else stream, exception_callback, owned))

    def start(self):
        """Connect to the local feed and start streaming the subscriptions."""
        if self._thread:
            raise BriskError('This session has started; a Live streams once')
        if not self._subscriptions:
            raise BriskError('Subscribe to something first')
        wanted = [s.symbols for s in self._subscriptions]
        request = {'version': VERSION, 'key': self._key, 'dataset': self.dataset,
                   'symbols': None if None in wanted else sorted(set().union(*wanted)),
                   'replay': any(s.start is not None for s in self._subscriptions),
                   'snapshot': any(s.snapshot for s in self._subscriptions)}
        try:
            self._sock = socket.create_connection((self._host, self._port), timeout=10)
            self._sock.sendall(_line(request))
            self._lines = self._sock.makefile('rb')
            header = _read_line(self._lines)
        except (OSError, ValueError) as error:
            if self._sock:
                self._sock.close()
            raise BriskError(f'No feed answers for {self.dataset} at {self._host}:{self._port} ({error}); is '
                             '`brisk live` still running?') from None
        if header.get('gateway') != 'session':
            self._sock.close()
            raise BriskError('The feed has ended' if header.get('gateway') == 'closed'
                             else f'The feed refused the session: {header.get("error")}')
        self._sock.settimeout(None)
        self._needles = None if request['symbols'] is None else \
            tuple(f'"code":"{code}"'.encode() for code in request['symbols'])
        self._thread = threading.Thread(target=self._run, args=(header,), name='brisk-live', daemon=True)
        self._thread.start()

    def __iter__(self):
        if self._queue is None:
            self._queue = queue.Queue(maxsize=10_000)
            if self._done.is_set():
                self._queue.put(_END)
        if not self._thread:
            self.start()
        while (item := self._queue.get()) is not _END:
            yield item
        if self._error:
            raise self._error

    def is_connected(self) -> bool:
        return bool(self._thread) and not self._done.is_set()

    def stop(self):
        """End the session: no more records are delivered."""
        self._stopping = True
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    terminate = stop

    def block_for_close(self, timeout=None):
        """Wait until the session ends (the feed ended or stop() was called); after `timeout` seconds, stop it.
        Raises what ended the session, if it failed."""
        if not self._thread:
            raise BriskError('This session has not started')
        if not self._done.wait(timeout):
            self.stop()
            self._done.wait()
        if self._error:
            raise self._error

    # The session's thread

    def _run(self, header):
        replaying, subs, ended = header['replay'], self._subscriptions, False
        try:
            if header['bootstrap'] is not None:
                self._new_day(header['bootstrap'])
                live = subs if not replaying else [s for s in subs if s.start is None]
                self._deliver(self._definitions(live))
                if header['snapshot'] is not None:
                    self._deliver(self._project(dict(self._day.header, quotes=header['snapshot']),
                                                [s for s in subs if s.snapshot], snapshot=True))
            for line in self._lines:
                if line.startswith(b'{"gateway"'):
                    control = json.loads(line)
                    if control['gateway'] == 'live':
                        replaying = False
                        continue
                    if control['gateway'] == 'error' or control.get('status') == 'failed':
                        raise BriskError(f'The feed ended the session: {control.get("error")}')
                    ended = True
                    break
                if self._needles and line.startswith((b'{"type":"quotes"', b'{"type":"history"')) \
                        and not any(needle in line for needle in self._needles):
                    continue
                batch = json.loads(line)
                if batch['type'] == 'bootstrap':
                    self._new_day(batch)
                if self._day is None:
                    continue
                takers = [s for s in subs if s.start is not None] if replaying else subs
                found = self._definitions(takers) if batch['type'] == 'bootstrap' else []
                if batch['type'] != 'end':
                    found += self._project(batch, takers)
                self._deliver(found)
            if not ended and not self._stopping:
                raise BriskError('The feed closed the connection without ending the session')
        except Exception as error:  # noqa: BLE001 - every failure ends the session visibly
            if not self._stopping:
                self._error = error if isinstance(error, BriskError) else BriskError(f'Live session failed: {error!r}')
        finally:
            self._sock.close()
            for stream, _, owned in self._streams:
                if owned:
                    stream.close()
            self._done.set()
            if self._queue is not None:
                self._put(_END, always=True)

    def _new_day(self, bootstrap):
        self._day = _Day(bootstrap)
        self.symbology_map = {records.instrument_id(m['code']): m['code'] for m in bootstrap['master']}

    def _definitions(self, takers):
        wanted = [s for s in takers if s.schema == 'definition']
        if not wanted or self._day is None:
            return []
        table = records.definition(self._day.equity, self._day.midnight, records.DATASETS[self.dataset][0]) \
            .sort_by([('instrument_id', 'ascending')])
        return [Record(row, 'definition') for s in wanted for row in _only(table, s.symbols).to_pylist()]

    def _project(self, batch, takers, snapshot=False):
        """The records of one batch for these subscriptions, ordered as Historical orders them."""
        day = self._day
        depths_wanted = any(s.schema in ('mbp-10', 'imbalance', 'brisk-quotes') for s in takers)
        # Trades are read whenever a subscription wants them, so trade IDs continue exactly after a replay.
        trades_wanted = not snapshot and any(s.schema == 'trades' for s in self._subscriptions)
        if not (batch.get('quotes') or batch.get('trades')) or not (depths_wanted or trades_wanted):
            return []
        batches = [dict(batch, master=[])] if batch['type'] == 'bootstrap' else [day.header, batch]
        depths, trades = _parquet.stream_tables(batches, None if snapshot else day.carry, depths_wanted, trades_wanted)
        publisher = records.DATASETS[self.dataset][0]
        received = batch.get('received_unix_ms') if self.dataset == 'XTKS.BRISK' else None
        found = []
        for index, s in enumerate(takers):
            if s.schema == 'definition' or (snapshot and not s.snapshot):
                continue
            table = trades if s.schema == 'trades' else depths
            if table is None or not table.num_rows:
                continue
            ts_recv = pa.array(np.full(table.num_rows, received * 10**6), records.TIME) if received else None
            out = _only(records.project(s.schema, table, publisher, ts_recv, day.equity), s.symbols)
            if s.start is not None:
                out = out.filter(pc.greater_equal(pc.cast(out.column('ts_event'), pa.int64()), s.start))
            out = out.sort_by([('ts_event', 'ascending'), ('instrument_id', 'ascending'), ('sequence', 'ascending')])
            times = pc.cast(out.column('ts_event'), pa.int64()).to_pylist()
            ids = out.column('instrument_id').to_pylist()
            found += [(t, i, index, Record(row, s.schema)) for t, i, row in zip(times, ids, out.to_pylist())]
        if len(takers) > 1:
            found.sort(key=lambda item: item[:3])
        return [item[3] for item in found]

    def _deliver(self, found):
        for record in found:
            for callback, on_error in self._callbacks:
                try:
                    callback(record)
                except Exception as error:  # noqa: BLE001 - a callback's failure is its own
                    _report(on_error, error, 'callback')
            if self._streams:
                data = json.dumps(record, default=lambda value: value.isoformat(), ensure_ascii=False) + '\n'
                for stream, on_error, _ in self._streams:
                    try:
                        stream.write(data if isinstance(stream, io.TextIOBase) else data.encode())
                    except Exception as error:  # noqa: BLE001
                        _report(on_error, error, 'stream')
            if self._queue is not None:
                self._put(record)
        for stream, on_error, _ in self._streams if found else ():
            try:
                stream.flush()    # each batch's records are readable as they arrive
            except Exception as error:  # noqa: BLE001
                _report(on_error, error, 'stream')

    def _put(self, item, always=False):
        # Backpressure, but never wedge the session on an abandoned iterator.
        while always or not self._stopping:
            try:
                return self._queue.put(item, timeout=0.2)
            except queue.Full:
                if always and self._stopping:
                    return


def _only(table, symbols):
    return table if symbols is None else table.filter(pc.is_in(table.column('symbol'), value_set=pa.array(sorted(symbols))))


def _report(on_error, error, where):
    if on_error is not None:
        on_error(error)
    else:
        warnings.warn(f'Live {where} failed: {error!r}')
