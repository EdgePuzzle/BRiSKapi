"""briskapi.Live: Databento-style live records from the feed running on this computer.

    live = briskapi.Live()                     # no key or sign-in: the local feed's own
    live.subscribe(dataset="XTKS.BRISK", schema="imbalance", symbols=["7203"])
    for record in live:
        print(record.symbol, record.ind_match_price)

A feed started with a gateway (`brisk live` does by default; briskapi.connect(gateway=True)) serves its
decoded batches to Live clients on this computer only. It listens on 127.0.0.1 and admits a client only
with the key it writes, with its port, to an owner-only file (~/.config/brisk/gateway/DATASET.json), which
Live reads. Each client turns the batches into the records the cloud API returns (briskapi.records), so
a record is the same live as in history, and builds OHLCV bars from the trades it receives. Only the feed
signs in to the broker.

The protocol (newline-delimited JSON, version 1). The client sends {"version", "key", "dataset"} and the
gateway answers {"gateway": "session"}. Then, at any time, the client sends {"subscribe": [ids], "symbols",
"replay", "snapshot"}, and the gateway answers in its stream, in order: {"gateway": "subscribed", "id",
"bootstrap", "snapshot", "replay"} with the trading day's bootstrap (master, no quotes) if the session lacks
it and, if asked, those symbols' current quotes; then, if asked and the day is running, the day so far
exactly as the decoder wrote it, and {"gateway": "live", "id"}. Every batch is passed on as it arrives,
with only the quotes and trades of the symbols subscribed so far, and last {"gateway": "closed"}. A client
that falls more than MAX_BEHIND bytes behind the feed is sent {"gateway": "error"} and dropped, so a slow
reader never slows the feed or its recording.

A broker feed's gateway has a fast lane. The recording reads the whole market's quotes frame by frame, and
in the opening auction they can fall behind the stream; the fast lane reads only the symbols of the Live
sessions on it, as each frame arrives, and they come ahead of the recording's batches (as batches of the
same numbers, with only those symbols). A session is on it from its first subscription if that asks for no
replay and, with the other sessions on it, for no more symbols than the lane takes; ALL_SYMBOLS never is.
The others, and a session whose later subscription no longer fits, get the recording's batches as before:
a session leaving the fast lane gets them from where the fast lane left it (and for its new symbols, from
its subscription's snapshot), so it misses nothing and gets nothing twice. A subscription that adds
symbols to the lane is answered once the lane has read their quotes.
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
import time
from typing import Callable
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
# How far back a session looks for the feed's delay: the largest in this window is the one it allows for.
LATENCY_WINDOW_NS = 10 * 10**9
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

    def __init__(self, dataset, on_fast=None):
        self.dataset, self.key = dataset, secrets.token_urlsafe(32)
        self._on_fast = on_fast         # tells the host which symbols the fast lane reads; None: no fast lane
        self._lanes = None              # the host's, said before each session's bootstrap: {'fast', 'capacity'}
        self._fast_quotes = {}          # issue_id -> its latest quote on the fast lane
        self._fast_seq = -1             # the batch the fast lane's quotes are at
        self._fast_codes = set()        # the symbols the host was last told
        self._fast_read = set()         # those it says it reads
        self._server = socket.create_server((HOST, 0))
        self._server.settimeout(0.25)
        self.port = self._server.getsockname()[1]
        self._lock = threading.Lock()
        self._clients: list[_Client] = []
        self._bootstrap = None   # the trading day's bootstrap batch, without quotes and trades
        self._quotes = {}        # issue_id -> its latest quote
        self._day = None         # [the feed's spool of the day's batches, how many it has published] while the day runs
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

    def publish(self, batch, line, spool=None):
        """Pass on a batch and its line as the decoder wrote it, the last that `spool` (if any) holds."""
        with self._lock:
            if self._closed:
                return
            if batch['type'] == 'bootstrap':
                self._bootstrap = {k: v for k, v in batch.items() if k not in ('quotes', 'trades')}
                self._quotes, self._day = {}, [spool, 0] if spool else None
                self._fast_quotes, self._fast_seq = {q['issue_id']: q for q in batch.get('quotes', ())}, batch['seq']
            if self._day:
                self._day[1] += 1
            for quote in batch.get('quotes', ()):
                self._quotes[quote['issue_id']] = quote
            if batch['type'] == 'end':
                self._day = None   # the feed packages and removes the day's spool
            for client in self._clients:
                if client.lane in ('fast', 'joining') and batch['type'] == 'quotes':
                    continue   # the fast lane brings them
                client.send(batch, line)
                if batch['type'] == 'bootstrap':
                    client.known, client.fast_until = self._bootstrap, None
                    if client.lane == 'joining':
                        self._answer(client)   # the bootstrap holds every quote

    def lane(self, batch, line):
        """Pass on a line of the fast lane: which lanes the host's session has, a joined symbol's quotes, or a
        frame's quotes of the lane's symbols."""
        with self._lock:
            if self._closed:
                return
            if batch['type'] == 'lanes':
                # A new session, before its bootstrap (nothing of it has reached a session yet): what it can take
                # and what it reads. The sessions waiting to know, and those on the lane, go on it if they fit.
                self._lanes = {'fast': batch['fast'], 'capacity': batch['capacity']}
                self._fast_read = set(batch['codes']) if batch['fast'] else set()
                fits = self._fits(set())
                for client in self._clients:
                    if client.lane in ('pending', 'fast') and fits:
                        client.lane, client.fast_codes, client.fast_until = 'fast', client.codes, None
                    elif client.lane in ('pending', 'fast', 'joining') and not fits:
                        self._fall_back(client, new_session=True)
                self._tell_host()
                return
            for quote in batch.get('quotes', ()):
                self._fast_quotes[quote['issue_id']] = quote
            self._fast_seq = batch['seq']
            if batch['type'] == 'join':
                self._fast_read |= set(batch['codes'])
                for client in self._clients:
                    if client.lane == 'joining' and client.codes <= self._fast_read:
                        self._answer(client)
                return
            for client in self._clients:
                if client.lane in ('fast', 'joining') and client.fast_codes:
                    client.send_fast(batch)
                    client.fast_seq = batch['seq']

    def _fast_union(self, extra=frozenset()):
        return set().union(*(c.codes for c in self._clients if c.lane in ('pending', 'fast', 'joining')), extra)

    def _fits(self, codes):
        """Whether the fast lane can take these symbols as well as its sessions'."""
        lanes = self._lanes
        if self._on_fast is None or codes is None or (lanes is not None and not lanes['fast']):
            return False
        return lanes is None or len(self._fast_union(codes)) <= lanes['capacity']

    def _tell_host(self):
        codes = self._fast_union()
        if self._on_fast is not None and codes != self._fast_codes:
            self._fast_codes = codes
            self._fast_read &= codes   # it stops reading the others
            self._on_fast(codes)

    def _answer(self, client, slow=False):
        """Answer a session's subscription that waited for the fast lane to read its new symbols (or, `slow`, for
        the recording's batches, if it left the lane first)."""
        ids, codes, snapshot = client.joining
        client.joining = None
        if not slow:
            client.lane, client.fast_codes, client.fast_seq = 'fast', client.codes, self._fast_seq
        bootstrap = self._bootstrap if client.known is not self._bootstrap else None
        client.known = self._bootstrap
        quotes = (self._quotes if slow else self._fast_quotes) if snapshot and self._bootstrap else None
        client.put(_Subscribed(ids, codes, bootstrap, None if quotes is None else list(quotes.values()), None,
                               without=client.old_codes if slow else None))

    def _fall_back(self, client, new_session=False):
        """Move a session from the fast lane to the recording's batches. Up to the batch the fast lane brought
        it, it gets them only for the symbols the lane did not (none, at a new session, which nothing has
        reached)."""
        if not new_session and client.fast_codes:
            client.fast_until, client.old_codes = client.fast_seq, set(client.fast_codes)
        client.lane, client.fast_codes = 'slow', set()
        if client.joining:
            self._answer(client, slow=True)
            client.lane = 'slow'

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
        client = None
        try:
            sock.settimeout(10)
            reader = sock.makefile('rb')
            try:
                request = _read_line(reader, MAX_REQUEST)
            except ValueError:
                request = None
            sock.settimeout(None)
            refused = self._refuse(request)
            if refused:
                return sock.sendall(_line({'gateway': 'error', 'error': refused}))
            if request.get('probe'):
                return sock.sendall(_line({'gateway': 'probe', 'dataset': self.dataset}))
            with self._lock:
                if self._closed:
                    return sock.sendall(self._closed)
                if len(self._clients) >= MAX_CLIENTS:
                    return sock.sendall(_line({'gateway': 'error', 'error': f'At most {MAX_CLIENTS} Live sessions at once'}))
                client = _Client(sock)
                client.put(_line({'gateway': 'session', 'version': VERSION, 'dataset': self.dataset}))
                self._clients.append(client)
            threading.Thread(target=self._listen, args=(client, reader), name='brisk-gateway-requests', daemon=True).start()
            client.run()
        except (OSError, ValueError):
            pass
        finally:
            if client:
                with self._lock:
                    self._clients.remove(client)
                    self._tell_host()   # its symbols may leave the fast lane
                client.discard()
            sock.close()

    def _refuse(self, request):
        if not isinstance(request, dict) or request.get('version') != VERSION:
            return f'This gateway speaks version {VERSION}; update briskapi'
        if not isinstance(request.get('key'), str) or not hmac.compare_digest(request['key'].encode(), self.key.encode()):
            return 'Wrong key'
        if not request.get('probe') and request.get('dataset') != self.dataset:
            return f'This gateway serves {self.dataset}'
        return None

    def _listen(self, client, reader):
        """A session's subscriptions, as it sends them; its leaving ends its sending too."""
        try:
            while True:
                message = _read_line(reader, MAX_REQUEST)
                ids = message.get('subscribe') if isinstance(message, dict) else None
                if not (isinstance(ids, list) and ids and all(type(i) is int for i in ids)):
                    return client.put(_line({'gateway': 'error', 'error': 'Expected {"subscribe": [ids], ...}'}), last=True)
                try:
                    codes = records.symbols_of(message.get('symbols'))
                except (TypeError, ValueError) as error:
                    return client.put(_line({'gateway': 'error', 'error': str(error)}), last=True)
                with self._lock:
                    # Taken under the lock that publish() holds: the answer comes after every batch the
                    # session had without these symbols, and before every batch it gets with them.
                    fits = client.lane != 'slow' and not message.get('replay') and \
                        self._fits(None if codes is None else (client.codes or set()) | codes)
                    if fits and self._lanes is not None and self._lanes['fast'] and client.lane != 'pending':
                        client.widen(codes)
                        client.lane = 'joining'
                        client.joining = (ids, codes, bool(message.get('snapshot')))
                        self._tell_host()
                        if self._bootstrap is None or client.codes <= self._fast_read:
                            self._answer(client)   # no day running (its bootstrap holds every quote), or read already
                        continue
                    if client.lane in ('fast', 'joining'):
                        self._fall_back(client)
                    # Before the host has said which lanes its session has (none if it never does, as the demo's),
                    # a session that would fit waits on the recording's batches to know.
                    client.lane = 'pending' if fits and self._lanes is None and self._bootstrap is None else 'slow'
                    client.widen(codes)
                    self._tell_host()
                    bootstrap = self._bootstrap if client.known is not self._bootstrap else None
                    client.known = self._bootstrap
                    quotes = list(self._quotes.values()) if message.get('snapshot') and self._bootstrap else None
                    replay = (self._day[0].replay(self._day[1])) if message.get('replay') and self._day else None
                    client.put(_Subscribed(ids, codes, bootstrap, quotes, replay, without=client.old_codes))
        except (OSError, ValueError):
            client.end()


def _filtered(batch, codes, without=None):
    """A batch's line with only the quotes and trades of these codes (None: all) but not of those `without` (the
    master whole, and as a recording's batch: no fast lane mark); None when nothing is left."""
    out = {k: v for k, v in batch.items() if k != 'lane'}
    keep = lambda code: (codes is None or code in codes) and not (without and code in without)  # noqa: E731
    for name in ('quotes', 'trades'):
        if name in batch:
            out[name] = [item for item in batch[name] if keep(item['code'])]
    if batch['type'] in ('quotes', 'history') and not out.get('quotes') and not out.get('trades'):
        return None
    return _line(out)


class _Subscribed:
    """The gateway's answer to a subscription, written out on the session's thread: the line saying so (with
    the bootstrap and quotes it needs), then the replay, if any."""

    def __init__(self, ids, codes, bootstrap, quotes, replay, without=None):
        self.ids, self.codes, self.bootstrap, self.quotes, self.replay = ids, codes, bootstrap, quotes, replay
        self.without = without or set()   # symbols the session has newer quotes of (from the fast lane)

    def send(self, sock):
        snapshot = None if self.quotes is None else \
            sorted((q for q in self.quotes if (self.codes is None or q['code'] in self.codes) and q['code'] not in self.without),
                   key=lambda q: q['issue_id'])
        sock.sendall(_line({'gateway': 'subscribed', 'id': self.ids, 'bootstrap': self.bootstrap, 'snapshot': snapshot,
                            'replay': self.replay is not None}))
        if self.replay:
            try:
                chunk = []
                for line in self.replay:
                    chunk.append(line)
                    if len(chunk) >= 256:
                        sock.sendall(b''.join(chunk))
                        chunk = []
                sock.sendall(b''.join(chunk))
            finally:
                self.close()
            sock.sendall(_line({'gateway': 'live', 'id': self.ids}))

    def close(self):
        if self.replay:
            self.replay.close()


class _Client:
    """One session's outbox: what publish() hands it is sent on the session's own thread."""

    def __init__(self, sock):
        self.sock, self.codes, self.known = sock, set(), None   # the symbols so far; the bootstrap it has
        # Its lane: 'new' until it subscribes, then 'fast' (or 'joining' while the fast lane reads its new
        # symbols' quotes: it gets the lane's batches of `fast_codes` meanwhile) or 'slow' for good.
        self.lane, self.joining, self.fast_codes, self.fast_seq = 'new', None, set(), -1
        # After leaving the fast lane: the recording's batches up to `fast_until` only without `old_codes`.
        self.fast_until, self.old_codes = None, set()
        self._items, self._behind, self._last = deque(), 0, False
        self._ready = threading.Condition()

    def widen(self, codes):
        self.codes = None if codes is None or self.codes is None else self.codes | codes

    def send(self, batch, line):
        without = self.old_codes if self.fast_until is not None and batch['type'] == 'quotes' \
            and batch['seq'] <= self.fast_until else None
        data = line if self.codes is None and without is None else _filtered(batch, self.codes, without)
        if data:
            self.put(data)

    def send_fast(self, batch):
        data = _filtered(batch, self.fast_codes)
        if data:
            self.put(data)

    def put(self, item, last=False):
        size = len(item) if isinstance(item, bytes) else 0
        with self._ready:
            if self._last:
                if isinstance(item, _Subscribed):
                    item.close()
                return
            if self._behind + size > MAX_BEHIND and not last:
                # A slow reader is dropped, never waited for: the feed and its recording go on.
                self._clear()
                item, size, last = _line({'gateway': 'error', 'error': 'This session fell more than '
                                          f'{MAX_BEHIND // 1024**2} MB behind the feed and was dropped'}), 0, True
                threading.Timer(10, self._stop).start()
            self._items.append(item)
            self._behind += size
            self._last = last
            self._ready.notify()

    def end(self):
        """The session left: send nothing more."""
        with self._ready:
            self._clear()
            self._items.append(None)
            self._last = True
            self._ready.notify()

    def discard(self):
        with self._ready:
            self._clear()

    def _clear(self):
        for item in self._items:
            if isinstance(item, _Subscribed):
                item.close()
        self._items.clear()
        self._behind = 0

    def _stop(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def run(self):
        while True:
            with self._ready:
                self._ready.wait_for(lambda: self._items)
                item = self._items.popleft()
                if isinstance(item, bytes):
                    self._behind -= len(item)
                last = self._last and not self._items
            if item is None:
                return
            if isinstance(item, bytes):
                self.sock.sendall(item)
            else:
                item.send(self.sock)
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


class _Subscription:
    def __init__(self, id, schema, symbols, start, snapshot):
        self.id, self.schema, self.symbols, self.start, self.snapshot = id, schema, symbols, start, snapshot
        self.active = False    # gets the live batches (once the gateway has confirmed it, after any replay)
        self.bars = _Bars(records.BARS[schema]) if schema in records.BARS else None


class _Bars:
    """One subscription's OHLCV bars in progress, built from its trades as the cloud builds them: the open is the
    first trade by (ts_event, frame, place in the frame), the close the last. A bar is given as soon as the
    feed's clock reaches its end, and only if the trades it saw cover it as the cloud's rule asks: its whole
    span within the trading sessions lies between `first` (where they begin, UTC ns) and the clock. With
    `first` None (a replay of the demo, whose cloud bars have no such rule) every bar is given. The clock is
    the latest batch's, or this computer's less the feed's delay (Live._tick). A trade that arrives after its
    own bar has ended goes into the next bar still open, so every trade counts and bars come in order (the
    cloud's bars place it by its own time)."""

    def __init__(self, length):
        self.length = length
        self.begin(None)
        self.started = False

    def begin(self, first):
        self.first, self.started = first, True
        self.open = {}         # bar start -> {code: [open key, open, high, low, close key, close, volume]}
        self.horizon = None    # the clock the bars have been given to
        self._spans = {}

    def add(self, trades):
        """Rows of the trades schema."""
        if not self.started or not trades.num_rows:
            return
        times = pc.cast(trades.column('ts_event'), pa.int64()).to_pylist()
        for code, ts, frame, trade, price, size in zip(trades.column('symbol').to_pylist(), times,
                                                      trades.column('sequence').to_pylist(),
                                                      trades.column('trade_id').to_pylist(),
                                                      trades.column('price').to_pylist(), trades.column('size').to_pylist()):
            start = (ts + records.JST_NS) // self.length * self.length - records.JST_NS
            while self.horizon is not None and self._span(start)[0] <= self.horizon:
                start += self.length    # its bar has ended: the next one still open
            key = (ts, frame, int(trade.split('-')[1]))
            bar = self.open.setdefault(start, {}).get(code)
            if bar is None:
                self.open[start][code] = [key, price, price, price, key, price, size]
                continue
            if key < bar[0]:
                bar[0], bar[1] = key, price
            if key >= bar[4]:
                bar[4], bar[5] = key, price
            bar[2], bar[3], bar[6] = max(bar[2], price), min(bar[3], price), bar[6] + size

    def _span(self, start):
        """Where a bar ends for its trades, and its session pieces ((low, high), UTC ns; None without the rule)."""
        if start not in self._spans:
            if self.first is None:
                self._spans[start] = start + self.length, None
            else:
                pieces = [(int(low[0]), int(high[0])) for low, high in
                          records.session_pieces(np.array([start]), np.array([start + self.length])) if high[0] > low[0]]
                self._spans[start] = max(high for _, high in pieces), pieces
        return self._spans[start]

    def due(self):
        """When the next bar ends (UTC ns, BRiSK's clock), or None."""
        return min((self._span(start)[0] for start in self.open), default=None)

    def ready(self, clock, final=False):
        """[(bar start, code, (open, high, low, close, volume))] for the bars done by `clock` (UTC ns), or, with
        `final` (the day's end), for every bar left."""
        self.horizon = clock if self.horizon is None else max(self.horizon, clock)
        out = []
        for start in sorted(self.open):
            end, pieces = self._span(start)
            covered = pieces is None or all(self.first <= low and high <= clock + 1 for low, high in pieces)
            if final or clock >= end:
                bars = self.open.pop(start)
                if covered:
                    out += [(start, code, (b[1], b[2], b[3], b[5], b[6])) for code, b in bars.items()]
        return out


class _Latency:
    """How far the feed's clock runs behind this computer's: for each live batch, when the session took it
    in less BRiSK's stamp (the network's delay, the hop through the gateway, and the two clocks' difference).
    The largest over the last LATENCY_WINDOW_NS is allowed for (a sliding maximum); once quiet, the last."""

    def __init__(self):
        self._ages = deque()   # (taken in, age), ages decreasing

    def add(self, taken, clock):
        age = taken - clock
        while self._ages and self._ages[-1][1] <= age:
            self._ages.pop()
        self._ages.append((taken, age))
        while self._ages[0][0] < taken - LATENCY_WINDOW_NS:
            self._ages.popleft()

    def clock(self, now):
        """The feed's clock as it stands here at `now` (UTC ns), or None before any live batch."""
        return now - self._ages[0][1] if self._ages else None


class _Day:
    """What projecting a trading day's batches needs: its bootstrap without market data, its equity table,
    and the trade IDs' places so far."""

    def __init__(self, bootstrap):
        self.header = {**bootstrap, 'master': [], 'quotes': [], 'trades': []}
        self.date = bootstrap['trading_date']
        self.equity = _parquet.equity_table(bootstrap)
        self.midnight = _parquet.midnight_ns(self.date)
        self.first = self.midnight + bootstrap['source_time_us'] * 1000
        self.carry = {}

    def clock(self, batch):
        return self.midnight + batch['source_time_us'] * 1000


class Live:
    """Databento-style live records from the feed on this computer (see the module docstring).

    Names follow databento.Live: subscribe(), then iterate, or add_callback() / add_stream(), start() and
    block_for_close(); subscribe() also works once the session runs. Records are briskapi.records' schemas, as
    Historical returns them, OHLCV bars included, in the order the feed receives them (within a batch, as
    Historical orders them). `start` replays the feed's trading day from that time first (as far back as the
    feed has it: from its own start, plus the day's trades its snapshot restored); `snapshot=True` begins with
    each security's current quote. A bar subscription gives the bars its trades cover: with `start`, from the
    feed's start; without, those that begin after it joined.
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
        self._needles = ()
        self._replay = None              # while one runs: [the subscriptions it is for, its own day]
        self._latency = _Latency()
        self._busy = threading.Lock()    # one batch, or one tick, at a time
        self._write = threading.Lock()
        self._stopping = False
        self._done = threading.Event()

    def __repr__(self):
        state = 'closed' if self._done.is_set() else 'streaming' if self._thread else 'ready'
        return f'Live(dataset={self.dataset!r}, {state})'

    def subscribe(self, dataset, schema, symbols='ALL_SYMBOLS', stype_in='raw_symbol', start=None, snapshot=False):
        """Ask for `schema`'s records of `symbols` (JPX codes), before or during the session. `start` (a time, a
        date or UTC ns; 0 for all the feed has) replays from then; `snapshot` starts mbp-10, imbalance or
        brisk-quotes with the current quotes."""
        if self._done.is_set():
            raise BriskError('This session has ended; start another Live')
        if dataset not in records.DATASETS:
            raise ValueError(f'Unknown dataset {dataset!r}; one of {", ".join(records.DATASETS)}')
        if self.dataset not in (None, dataset):
            raise ValueError(f'This session streams {self.dataset}; use another Live for {dataset}')
        if schema not in records.SCHEMAS:
            raise ValueError(f'Unknown schema {schema!r}; one of {", ".join(records.SCHEMAS)}')
        if stype_in not in (None, '', 'raw_symbol'):
            raise ValueError('Only stype_in=raw_symbol (JPX codes) is supported')
        if snapshot and start is not None:
            raise ValueError('Choose start (a replay) or snapshot (the current quotes), not both')
        if snapshot and schema not in ('mbp-10', 'imbalance', 'brisk-quotes'):
            raise ValueError('snapshot applies to mbp-10, imbalance and brisk-quotes')
        info = None if self._key and self._port else _read_file(gateway_path(dataset))
        if info is None and not (self._key and self._port):
            how = '`brisk live --site sbi` (or your broker)' if dataset == 'XTKS.BRISK' else '`brisk live --site demo`'
            raise BriskError(f'No feed on this computer serves {dataset}: start one with {how}, '
                             'or briskapi.connect(..., gateway=True)')
        self._key, self._port = self._key or info['key'], self._port or info['port']
        self.dataset = dataset
        subscription = _Subscription(len(self._subscriptions), schema, records.symbols_of(symbols),
                                     None if start is None else records.timestamp(start), bool(snapshot))
        self._subscriptions.append(subscription)
        if self._thread:
            self._ask([subscription])

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
        try:
            self._sock = socket.create_connection((self._host, self._port), timeout=10)
            self._sock.sendall(_line({'version': VERSION, 'key': self._key, 'dataset': self.dataset}))
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
        self._thread = threading.Thread(target=self._run, name='brisk-live', daemon=True)
        self._thread.start()
        threading.Thread(target=self._tick, name='brisk-live-bars', daemon=True).start()
        self._ask(self._subscriptions)

    def _ask(self, subscriptions):
        """Send subscriptions (one request: one replay at most); replayed lines of other symbols are skipped
        unread."""
        wanted = [s.symbols for s in self._subscriptions]
        self._needles = None if None in wanted else tuple(f'"code":"{c}"'.encode() for c in sorted(set().union(*wanted)))
        symbols = [s.symbols for s in subscriptions]
        try:
            with self._write:
                self._sock.sendall(_line({'subscribe': [s.id for s in subscriptions],
                                          'symbols': None if None in symbols else sorted(set().union(*symbols)),
                                          'replay': any(s.start is not None for s in subscriptions),
                                          'snapshot': any(s.snapshot for s in subscriptions)}))
        except OSError:
            raise BriskError('This session has ended; start another Live') from None

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

    def _run(self):
        ended = False
        try:
            for line in self._lines:
                taken = time.time_ns()
                if line.startswith(b'{"gateway"'):
                    control = json.loads(line)
                    if control['gateway'] == 'error' or control.get('status') == 'failed':
                        raise BriskError(f'The feed ended the session: {control.get("error")}')
                    if control['gateway'] == 'closed':
                        ended = True
                        break
                    with self._busy:
                        if control['gateway'] == 'subscribed':
                            self._replay = self._subscribed(control)
                        else:   # live: the replay is over
                            for s in self._replay[0]:
                                s.active = True
                            self._replay = None
                    continue
                if self._needles and line.startswith((b'{"type":"quotes"', b'{"type":"history"')) \
                        and not any(needle in line for needle in self._needles):
                    continue
                with self._busy:
                    self._take(json.loads(line), taken)
            if not ended and not self._stopping:
                raise BriskError('The feed closed the connection without ending the session')
        except Exception as error:  # noqa: BLE001 - every failure ends the session visibly
            if isinstance(error, OSError):   # a reset, as when the feed's process is killed
                error = BriskError(f'The feed closed the connection without ending the session ({error})')
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

    def _take(self, batch, taken):
        """One batch: the records and bars it gives, to the subscriptions it is for."""
        replay = self._replay
        if replay is not None:
            if batch['type'] == 'bootstrap':
                replay[1] = _Day(batch)
            day, takers = replay[1], replay[0]
        else:
            if batch['type'] == 'bootstrap':
                self._new_day(batch)
            day, takers = self._day, [s for s in self._subscriptions if s.active]
        if day is None:
            return
        if replay is None:
            self._latency.add(taken, day.clock(batch))
        whole = replay is not None or batch['type'] == 'bootstrap'
        for s in takers:
            if s.bars and (batch['type'] == 'bootstrap' or not s.bars.started):
                # Seen from the day's start (a replay, or a new day), bars are covered as the cloud's (the
                # demo's: every bar); joined later, from the first batch seen.
                s.bars.begin(None if whole and self.dataset == 'XTKS.BRISKDEMO' else day.first if whole else day.clock(batch))
        self._deliver((self._definitions(takers, day) if batch['type'] == 'bootstrap' else [])
                      + self._project(batch, takers, day, replayed=replay is not None))

    def _tick(self):
        """Give the bars whose end the feed's clock has passed by this computer's, allowing for the feed's delay,
        when no later batch has said so: a quiet symbol's bar is not held until its next update."""
        wait = 0.25
        while not self._done.wait(wait):
            wait = 0.25
            with self._busy:
                bars = [s for s in self._subscriptions if s.bars and s.active and s.bars.started]
                clock = self._latency.clock(time.time_ns())
                if self._replay is not None or self._day is None or clock is None or not bars:
                    continue   # during a replay, live batches wait behind it: the clock says nothing of them
                publisher, found = records.DATASETS[self.dataset][0], []
                for index, s in enumerate(bars):
                    found += _ordered(_bar_table(s.bars.ready(clock), publisher), s, index)
                found.sort(key=lambda item: item[:3])
                self._deliver([item[3] for item in found])
                due = [d for d in (s.bars.due() for s in bars) if d is not None]
                if due:
                    wait = min(0.25, max(0.001, (min(due) - clock) / 1e9))

    def _subscribed(self, control):
        """Take the gateway's answer to a subscription: what the subscriptions start with, and the replay to come."""
        group = [self._subscriptions[i] for i in control['id']]
        if control['bootstrap'] is not None:
            self._new_day(control['bootstrap'])
        replaying = [s for s in group if s.start is not None] if control['replay'] else []
        live = [s for s in group if s not in replaying]
        for s in live:
            s.active = True
        found = self._definitions(live, self._day)
        if control['snapshot'] is not None and self._day is not None:
            found += self._project(dict(self._day.header, quotes=control['snapshot']),
                                   [s for s in group if s.snapshot], self._day, snapshot=True)
        self._deliver(found)
        return [replaying, None] if replaying else None

    def _new_day(self, bootstrap):
        self._day = _Day(bootstrap)
        self.symbology_map = {records.instrument_id(m['code']): m['code'] for m in bootstrap['master']}

    def _definitions(self, takers, day):
        wanted = [s for s in takers if s.schema == 'definition']
        if not wanted or day is None:
            return []
        table = records.definition(day.equity, day.midnight, records.DATASETS[self.dataset][0]) \
            .sort_by([('instrument_id', 'ascending')])
        return [Record(row, 'definition') for s in wanted for row in _only(table, s.symbols).to_pylist()]

    def _project(self, batch, takers, day, snapshot=False, replayed=False):
        """One batch's records for these subscriptions, ordered as Historical orders them, and the bars it
        completes."""
        quoted = ('mbp-10', 'imbalance', 'brisk-quotes')
        depths_wanted = any(s.schema in quoted for s in takers)
        # Trades are read whenever a subscription wants them (on the live day, any), so trade IDs carry on exactly.
        traders = takers if replayed else self._subscriptions
        trades_wanted = not snapshot and any(s.schema == 'trades' or s.bars for s in traders)
        publisher = records.DATASETS[self.dataset][0]
        found = []
        if (batch.get('quotes') or batch.get('trades')) and (depths_wanted or trades_wanted):
            batches = [dict(batch, master=[])] if batch['type'] == 'bootstrap' else [day.header, batch]
            depths, trades = _parquet.stream_tables(batches, None if snapshot else day.carry, depths_wanted, trades_wanted)
            received = batch.get('received_unix_ms') if self.dataset == 'XTKS.BRISK' else None
            for index, s in enumerate(takers):
                table = trades if s.schema == 'trades' or s.bars else depths if s.schema in quoted else None
                if table is None or not table.num_rows or (snapshot and not s.snapshot):
                    continue
                ts_recv = pa.array(np.full(table.num_rows, received * 10**6), records.TIME) if received else None
                out = _only(records.project('trades' if s.bars else s.schema, table, publisher, ts_recv, day.equity), s.symbols)
                if s.bars:
                    s.bars.add(out)
                    continue
                found += _ordered(out, s, index)
        if not snapshot:
            clock = day.clock(batch)
            for index, s in enumerate(takers):
                if s.bars:
                    found += _ordered(_bar_table(s.bars.ready(clock, final=batch['type'] == 'end'), publisher), s, index)
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


def _bar_table(bars, publisher):
    """Bars from _Bars.ready as records of an OHLCV schema."""
    starts, codes, values = zip(*bars) if bars else ((), (), ())
    opens, highs, lows, closes, volumes = zip(*values) if bars else ((),) * 5
    return pa.table({'ts_event': pa.array(starts, records.TIME), 'publisher_id': pa.array([publisher] * len(starts), pa.uint16()),
                     'instrument_id': pa.array([records.instrument_id(c) for c in codes], pa.uint32()),
                     'open': pa.array(opens, pa.float64()), 'high': pa.array(highs, pa.float64()),
                     'low': pa.array(lows, pa.float64()), 'close': pa.array(closes, pa.float64()),
                     'volume': pa.array(volumes, pa.uint64()), 'symbol': pa.array(codes, pa.string())})


def _ordered(table, subscription, index):
    """(ts_event, instrument_id, subscription index, record) for a subscription's records from `start` on, in
    Historical's order."""
    if not table.num_rows:
        return []
    if subscription.start is not None:
        table = table.filter(pc.greater_equal(pc.cast(table.column('ts_event'), pa.int64()), subscription.start))
    keys = [('ts_event', 'ascending'), ('instrument_id', 'ascending')]
    table = table.sort_by(keys + ([('sequence', 'ascending')] if 'sequence' in table.column_names else []))
    times = pc.cast(table.column('ts_event'), pa.int64()).to_pylist()
    schema = subscription.schema
    return [(t, i, index, Record(row, schema))
            for t, i, row in zip(times, table.column('instrument_id').to_pylist(), table.to_pylist())]


def _report(on_error, error, where):
    if on_error is not None:
        on_error(error)
    else:
        warnings.warn(f'Live {where} failed: {error!r}')
