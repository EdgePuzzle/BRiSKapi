"""Recordings as Parquet: NautilusTrader v2's market data tables, extended with BRiSK's own fields.

A recording directory holds four tables:

data/order_book_depths.parquet  One row per quote update: v2's OrderBookDepth (each side's best levels, the
                                frame number as `sequence`, the source time as `ts_event`), then the rest of
                                BRiSK's quote. Sorted by issue, then batch.
data/trades.parquet             One row per execution: v2's TradeTick, then the decoder's raw side, frame and
                                batch. Sorted by issue, then in the decoder's order.
data/equity.parquet             The securities master: v2's Equity, then BRiSK's master fields.
data/custom/BriskBatch.parquet  One row per decoded batch, laid out as v2 custom data: local timing, skipped
                                issues and, on the bootstrap row, the session's provenance.

Rows carry v2's `identifier` column (CODE.XTKS), as v2 returns multi-instrument queries, instead of a
directory per instrument; `export_nautilus` writes that catalog layout. Prices and sizes are
decimal128(38, 16) as in v2, and the vendor's zero price sentinel is null. `ts_init` is the source time:
local receipt clocks stay exact in BriskBatch. Reading gives back the decoder's batches exactly, so the
archive's checks and reference fingerprints apply unchanged.
"""
from __future__ import annotations

from array import array
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import hashlib
import io
import json
import operator
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import threading
import zipfile

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from briskapi.schema import (BATCH_KEYS, BROKER_TRANSPORT, LEVELS, MASTER_KEYS, MAX_COMPRESSED, MAX_DECODE_NS, MAX_HISTORY,
                             MAX_SESSION_MS, QUOTE_KEYS, SCHEMA, SKIPPED, TRADE_KEYS, UNIX_MS, canonical, canonical_batches,
                             code, encode, integer, measurement, require, validate_batches, validate_manifest)

VENUE = 'XTKS'
EQUITY, DEPTHS, TRADES, BATCHES = ('data/equity.parquet', 'data/order_book_depths.parquet', 'data/trades.parquet',
                                   'data/custom/BriskBatch.parquet')
FILES = (EQUITY, DEPTHS, TRADES, BATCHES)
# The market data; a demo replay's is the same for everyone, so the archive keeps it once.
MARKET = (EQUITY, DEPTHS, TRADES)
ROW_GROUP = 65_536
ZSTD_LEVEL = 3
F_MBP, F_SNAPSHOT = 16, 32  # NautilusTrader record flags: market by price; the session's opening snapshot
MAX_ROWS = {EQUITY: 20_000, DEPTHS: 4_000_000, TRADES: 20_000_000, BATCHES: 4_000_000}
MAX_DECODED = 2 * 1024**3
# A broker's recording (a trading session of the whole market) is checked a row group at a time (verify_broker),
# so it may be far larger: a session holds some 25 million quote updates.
BROKER_MAX_ROWS = {EQUITY: 20_000, DEPTHS: 100_000_000, TRADES: 50_000_000, BATCHES: 10_000_000}
BROKER_MAX_DECODED = 256 * 1024**3
JST = dt.timezone(dt.timedelta(hours=9))
# Building columns and summaries is Python for every row; without the GIL (free-threaded Python) it is spread over
# _THREADS threads, a spool part in slices of at least SLICE_LINES lines. A third of the cores, at most 4: the live
# feed and its decoder host run meanwhile, and come first (8 slowed the demo's stream by two thirds, 4 by a tenth).
_THREADS = min(4, max(1, (os.cpu_count() or 1) // 3))
SLICE_LINES = 16
# A spool part's depths hold a row group per PART_ISSUES issue IDs; a piece is written PIECE_ROWS quote rows (about
# 0.5 GB in memory) at a time, a range of issues from every part, so no day is ever whole in memory.
PART_ISSUES = 32
PIECE_ROWS = 400_000
_pool, _pool_lock = None, threading.Lock()


def _threads():
    """The shared pool, or None with the GIL on. Its tasks never wait for one another."""
    global _pool
    if getattr(sys, '_is_gil_enabled', lambda: True)():   # the GIL is on, or (before 3.13, as in the archive's function) always
        return None
    with _pool_lock:
        _pool = _pool or ThreadPoolExecutor(_THREADS, thread_name_prefix='brisk-parquet')
    return _pool


def _map(function, items):
    """[function(item) for item in items], on threads when the GIL is off."""
    items = list(items)
    pool = _threads() if len(items) > 1 else None
    return list(pool.map(function, items)) if pool else [function(item) for item in items]

DECIMAL = pa.decimal128(38, 16)
TIME = pa.timestamp('ns', tz='UTC')
LEVEL = pa.struct([pa.field('price', DECIMAL, False), pa.field('size', DECIMAL, False),
                   pa.field('count', pa.uint32(), False), pa.field('order_id', pa.uint64(), False)])
BOOK = pa.list_(pa.field('item', LEVEL, False))

# The decoder's quote, in its own order. Prices are tenths of a yen and times microseconds after
# JST midnight; zero means none.
QUOTE_ORDER = ('issue_id', 'code', 'frame', 'max_frame', 'source_time_us', 'last_price10', 'open_price10', 'bid_price10',
               'ask_price10', 'indicative_price10', 'indicative_volume', 'indicative_imbalance_side',
               'closing_indicative_price10', 'closing_indicative_volume', 'closing_indicative_imbalance_side',
               'quote_flag', 'quote_side', 'special_quote_time_us', 'indicative_open_price10', 'reference_price10',
               'volume', 'turnover10', 'issue_status', 'market_buy_quantity', 'market_sell_quantity',
               'closing_market_buy_quantity', 'closing_market_sell_quantity', 'bid_quantity', 'bid_orders',
               'ask_quantity', 'ask_orders', 'bid_levels', 'ask_levels', 'bid_closing_levels', 'ask_closing_levels',
               'bid_total_quantity', 'bid_total_orders', 'bid_closing_total_quantity', 'bid_closing_total_orders',
               'ask_total_quantity', 'ask_total_orders', 'ask_closing_total_quantity', 'ask_closing_total_orders')
# Book sides and their columns: v2's bids and asks, and the closing-condition orders' books.
BOOKS = {'bid_levels': 'bids', 'ask_levels': 'asks', 'bid_closing_levels': 'closing_bids', 'ask_closing_levels': 'closing_asks'}
PRICES = tuple(k for k in QUOTE_ORDER if k.endswith('_price10'))
COUNTS = tuple(k for k in QUOTE_ORDER if k not in {'issue_id', 'code', 'frame', 'source_time_us', 'special_quote_time_us',
                                                   'turnover10', *PRICES, *BOOKS})
# Integer columns held in memory: the quote's, plus the batch each update arrived in.
INTS = ('seq', *(k for k in QUOTE_ORDER if k not in {'code', *BOOKS}))
TRADE_INTS = ('seq', 'issue_id', 'frame', 'time_us', 'price10', 'quantity', 'side')
def _split(seqs, flat, keys):
    """Columns (`keys`, the first `seq`) of each row's batch and its other integers, given row after row."""
    n = len(seqs)
    rows = np.frombuffer(flat, np.int64).reshape(n, len(keys) - 1) if n else np.zeros((0, len(keys) - 1), np.int64)
    return {'seq': np.frombuffer(seqs, np.int64) if n else np.zeros(0, np.int64),
            **{k: np.ascontiguousarray(rows[:, i]) for i, k in enumerate(keys[1:])}}
# The decoder's trade types as v2's AggressorSide.
AGGRESSOR = {1: 'BUY', 2: 'SELL'}
assert set(QUOTE_ORDER) == QUOTE_KEYS and set(TRADE_INTS[1:]) | {'code'} == TRADE_KEYS

DEPTH_SCHEMA = pa.schema([
    # NautilusTrader v2 OrderBookDepth, plus the identifier of its multi-instrument queries
    pa.field('bids', BOOK, False), pa.field('asks', BOOK, False), pa.field('flags', pa.uint8(), False),
    pa.field('sequence', pa.uint64(), False), pa.field('ts_event', TIME, False), pa.field('ts_init', TIME, False),
    pa.field('identifier', pa.string(), False),
    # BRiSK's quote beyond the book
    pa.field('issue_id', pa.uint64(), False), pa.field('seq', pa.uint64(), False),
    *[pa.field(k[:-2], DECIMAL) for k in PRICES],
    *[pa.field(k, pa.uint64(), False) for k in COUNTS],
    pa.field('special_quote_time', TIME), pa.field('turnover', DECIMAL, False),
    pa.field('closing_bids', BOOK, False), pa.field('closing_asks', BOOK, False),
], metadata={'type_name': 'OrderBookDepth', 'price_precision': '1', 'size_precision': '0', 'brisk_format': SCHEMA})
V2_DEPTH = DEPTH_SCHEMA.names[:6]

TRADE_SCHEMA = pa.schema([
    # NautilusTrader v2 TradeTick, plus the identifier of its multi-instrument queries
    pa.field('price', DECIMAL, False), pa.field('size', DECIMAL, False),
    pa.field('aggressor_side', pa.dictionary(pa.int8(), pa.string()), False), pa.field('trade_id', pa.string(), False),
    pa.field('ts_event', TIME, False), pa.field('ts_init', TIME, False), pa.field('identifier', pa.string(), False),
    # The decoder's own: its trade type, the issue's frame number and the batch
    pa.field('issue_id', pa.uint64(), False), pa.field('seq', pa.uint64(), False), pa.field('frame', pa.uint64(), False),
    pa.field('side', pa.uint8(), False),
], metadata={'type_name': 'TradeTick', 'price_precision': '1', 'size_precision': '0', 'brisk_format': SCHEMA})
V2_TRADE = TRADE_SCHEMA.names[:6]

EQUITY_SCHEMA = pa.schema([
    # NautilusTrader v2 Equity (`info`, JSON in v2, is a plain string here and always null)
    pa.field('id', pa.string(), False), pa.field('raw_symbol', pa.string(), False), pa.field('currency', pa.string(), False),
    pa.field('price_precision', pa.uint8(), False), pa.field('price_increment', pa.string(), False),
    *[pa.field(k, pa.string()) for k in ('lot_size', 'isin', 'max_quantity', 'min_quantity', 'max_price', 'min_price')],
    pa.field('margin_init', pa.string(), False), pa.field('margin_maint', pa.string(), False),
    pa.field('tick_scheme', pa.string()), pa.field('info', pa.string()),
    pa.field('ts_event', TIME, False), pa.field('ts_init', TIME, False),
    # BRiSK's master beyond v2's Equity
    pa.field('issue_id', pa.uint64(), False), pa.field('tick_type', pa.uint64(), False),
    pa.field('issue_type', pa.uint64(), False), pa.field('name', pa.string(), False), pa.field('base_price', DECIMAL),
], metadata={'type_name': 'Equity', 'brisk_format': SCHEMA})
V2_EQUITY = EQUITY_SCHEMA.names[:17]

# Batch fields by kind (beyond type, seq and source_time_us): columns are null where a kind has none.
BATCH_FIELDS = {kind: sorted(keys - {'type', 'seq', 'source_time_us', 'master', 'quotes', 'trades'}) for kind, keys in BATCH_KEYS.items()}
BATCH_TYPES = {'received_unix_ms': pa.uint64(), 'decode_ns': pa.uint64(), 'replay_lateness_ms': pa.float64(),
               SKIPPED: pa.list_(pa.field('item', pa.uint64(), False)), 'frames': pa.uint64(), 'quote_updates': pa.uint64(),
               'replay_wall_ms': pa.float64(), 'source': pa.string(), 'trading_date': pa.string(),
               'input_transport': pa.string(), 'source_timestamp_origin': pa.string(), 'exchange_delay_ms': pa.float64(),
               'market_issue_count': pa.uint64()}
BATCH_SCHEMA = pa.schema([
    pa.field('seq', pa.uint64(), False), pa.field('kind', pa.string(), False),
    *[pa.field(k, t) for k, t in BATCH_TYPES.items()],
    # v2 custom data
    pa.field('type', pa.string(), False), pa.field('ts_event', TIME, False), pa.field('ts_init', TIME, False),
], metadata={'type_name': 'BriskBatch', 'brisk_format': SCHEMA})
assert {k for keys in BATCH_FIELDS.values() for k in keys} | {SKIPPED} == set(BATCH_TYPES)

SCHEMAS = {EQUITY: EQUITY_SCHEMA, DEPTHS: DEPTH_SCHEMA, TRADES: TRADE_SCHEMA, BATCHES: BATCH_SCHEMA}
WRITE = dict(compression='zstd', compression_level=ZSTD_LEVEL, use_dictionary=True, write_statistics=True,
             version='2.6', data_page_version='1.0', use_compliant_nested_type=False)  # list items named 'item', as v2 writes them


def midnight_ns(date: str) -> int:
    """UTC nanoseconds at JST midnight on a YYYYMMDD trading date."""
    require(isinstance(date, str) and re.fullmatch(r'\d{8}', date), 'Invalid date')
    return int(dt.datetime.strptime(date, '%Y%m%d').replace(tzinfo=JST).timestamp()) * 10**9


def _decimal(values, scale, nulls=False):
    """decimal128(38, 16) from non-negative integers in units of 10**-scale (zero is null with `nulls`)."""
    values = np.ascontiguousarray(values, dtype=np.int64)
    require(not len(values) or values.min() >= 0, 'Invalid negative value')
    data = np.zeros((len(values), 2), np.uint64)
    data[:, 0] = values
    valid = values != 0
    bitmap = pa.py_buffer(np.packbits(valid, bitorder='little')) if nulls else None
    exact = pa.Array.from_buffers(pa.decimal128(38, scale), len(values), [bitmap, pa.py_buffer(data)],
                                  null_count=int(len(values) - valid.sum()) if nulls else 0)
    return exact.cast(DECIMAL)


def _integers(array, scale):
    """Integers in units of 10**-scale from decimal128(38, 16); finer digits are refused, null is zero."""
    out = []
    for chunk in (array.chunks if isinstance(array, pa.ChunkedArray) else [array]):
        exact = chunk.cast(pa.decimal128(38, scale))  # safe cast: refuses to drop non-zero digits
        raw = np.frombuffer(exact.buffers()[1], np.uint64).reshape(-1, 2)[exact.offset:exact.offset + len(exact)].copy()
        if exact.null_count:
            raw[_nulls(exact)] = 0
        require(not len(raw) or (raw[:, 1].max() == 0 and raw[:, 0].max() < 2**53), 'Invalid decimal value')
        out.append(raw[:, 0].astype(np.int64))
    return np.concatenate(out) if out else np.zeros(0, np.int64)


def _numbers(array, dtype=np.int64):
    """Non-null integers or timestamps (as nanoseconds), nulls as zero."""
    array = array.cast(pa.int64()) if pa.types.is_timestamp(array.type) else array
    values = array.fill_null(0).to_numpy(zero_copy_only=False)
    require(values.dtype != np.uint64 or not len(values) or values.max() < 2**53, 'Invalid integer value')
    return values.astype(dtype)


def _nulls(array):
    return np.asarray(array.is_null().to_numpy(zero_copy_only=False), dtype=bool)


def _price_text(tenths: int):
    return None if tenths == 0 else f'{tenths // 10}.{tenths % 10}'


def _tenths(text) -> int:
    if text is None:
        return 0
    match = re.fullmatch(r'(\d{1,16})\.(\d)', text)
    require(match, 'Invalid price text')
    return int(match[1]) * 10 + int(match[2])


class Columns:
    """A recording held as numpy columns in vendor units, sorted by issue then batch.

    `q` maps each integer quote field (and `seq`, the batch) to an int64 array; `code` holds the codes;
    `books` holds each side as (offsets, levels) with levels an (n, 3) array of [price10, quantity, orders].
    Columns built from batches keep arrival order with `order` sorting them, applied as they are written.
    `t` holds the trades (TRADE_INTS, `t_code` their codes), sorted by issue then the decoder's order.
    """

    def __init__(self, q, code, books, master, batches, t, t_code, order=None):
        self.q, self.code, self.books, self.master, self.batches, self.order = q, code, books, master, batches, order
        self.t, self.t_code = t, t_code

    def __len__(self):
        return len(self.code)

    # Building

    @classmethod
    def from_batches(cls, batches):
        """Columns from decoder batches, refusing unknown or missing fields."""
        # A quote's integers go into one row-major array, taken in C by an itemgetter, then split into columns.
        seqs, flat, code = array('q'), array('q'), []
        # Each code once (as interning would keep it, but sys.intern is one lock for every thread); and this call's
        # own key sets, as a set shared between threads is locked on every use.
        codes, quote_keys, trade_keys = {}, set(QUOTE_KEYS), set(TRADE_KEYS)
        batch_keys = {kind: set(keys) for kind, keys in BATCH_KEYS.items()}
        quote_ints, trade_ints, book_sides = (operator.itemgetter(*INTS[1:]), operator.itemgetter(*TRADE_INTS[1:]),
                                              operator.itemgetter(*BOOKS))
        books = {side: (array('q'), array('q')) for side in BOOKS}
        sides = list(books.values())
        t_seqs, t_flat, t_code = array('q'), array('q'), []
        rows, master = [], None
        try:
            for b in batches:
                require(isinstance(b, dict) and b.get('type') in batch_keys and set(b) - {SKIPPED} == batch_keys[b['type']],
                        'Unknown or missing batch fields')
                rows.append({k: v for k, v in b.items() if k not in ('master', 'quotes')})
                if b['type'] == 'bootstrap' and master is None:
                    master = b['master']
                    require(isinstance(master, list) and all(isinstance(m, dict) and set(m) == MASTER_KEYS for m in master),
                            'Unknown or missing master fields')
                quotes = b.get('quotes', ())
                for q in quotes:
                    require(isinstance(q, dict) and q.keys() == quote_keys, 'Unknown or missing quote fields')
                    flat.extend(quote_ints(q))
                    require(isinstance(q['code'], str), 'Invalid security code')
                    code.append(codes.setdefault(q['code'], q['code']))
                    for (counts, levels), side in zip(sides, book_sides(q)):
                        require(isinstance(side, list), 'Invalid book side')
                        counts.append(len(side))
                        for level in side:
                            if type(level) is not list or len(level) != 3:
                                raise ValueError('Invalid book level')
                            levels.extend(level)
                seqs.extend([b['seq']] * len(quotes))
                trades = b.get('trades', ())
                for t in trades:
                    require(isinstance(t, dict) and t.keys() == trade_keys, 'Unknown or missing trade fields')
                    t_flat.extend(trade_ints(t))
                    require(isinstance(t['code'], str), 'Invalid security code')
                    t_code.append(codes.setdefault(t['code'], t['code']))
                t_seqs.extend([b['seq']] * len(trades))
        except (TypeError, OverflowError) as error:
            raise ValueError(f'Invalid recording value: {error}') from None
        require(rows and rows[0]['type'] == 'bootstrap' and master is not None, 'Recording has no bootstrap')
        q, t = _split(seqs, flat, INTS), _split(t_seqs, t_flat, TRADE_INTS)
        t_order = np.lexsort((np.arange(len(t_code)), t['seq'], t['issue_id']))
        t, t_code = {k: v[t_order] for k, v in t.items()}, np.array(t_code, dtype=object)[t_order]
        code = np.array(code, dtype=object)
        sides = {}
        for side, (counts, levels) in books.items():
            counts = np.frombuffer(counts, np.int64) if len(counts) else np.zeros(0, np.int64)
            sides[side] = (np.concatenate([[0], np.cumsum(counts)]), (np.frombuffer(levels, np.int64) if len(levels)
                                                                         else np.zeros(0, np.int64)).reshape(-1, 3))
        master = sorted(master, key=lambda m: m['issue_id'] if type(m['issue_id']) is int else -1)
        return cls(q, code, sides, master, rows, t, t_code, order=np.lexsort((q['seq'], q['issue_id'])))

    def sorted(self):
        return self if self.order is None else self.take(self.order)

    def take(self, idx):
        """These rows, in this order."""
        books = {}
        for side, (offsets, levels) in self.books.items():
            counts = offsets[1:][idx] - offsets[:-1][idx]
            new = np.concatenate([[0], np.cumsum(counts)])
            gather = np.arange(new[-1]) + np.repeat(offsets[:-1][idx] - new[:-1], counts)
            books[side] = (new, levels[gather])
        return Columns({k: v[idx] for k, v in self.q.items()}, self.code[idx], books, self.master, self.batches,
                       self.t, self.t_code)

    # Writing

    @property
    def trading_date(self):
        return self.batches[0].get('trading_date')

    def write(self, directory):
        """Write the three tables under `directory`."""
        directory = Path(directory)
        for name in FILES:
            (directory / name).parent.mkdir(parents=True, exist_ok=True)
        midnight = midnight_ns(self.trading_date)
        try:
            pq.write_table(self._equity(midnight), directory / EQUITY, row_group_size=ROW_GROUP, **WRITE)
            pq.write_table(self._batches(midnight), directory / BATCHES, row_group_size=ROW_GROUP, **WRITE)
            order = np.arange(len(self)) if self.order is None else self.order
            with pq.ParquetWriter(directory / DEPTHS, DEPTH_SCHEMA, **WRITE) as writer:
                for start in range(0, len(self), ROW_GROUP):
                    writer.write_table(self.take(order[start:start + ROW_GROUP])._depths(midnight), row_group_size=ROW_GROUP)
            with pq.ParquetWriter(directory / TRADES, TRADE_SCHEMA, **WRITE) as writer:
                for start in range(0, len(self.t_code), ROW_GROUP):
                    writer.write_table(self._trades(midnight, slice(start, start + ROW_GROUP)), row_group_size=ROW_GROUP)
        except (TypeError, OverflowError) as error:  # Arrow's own type errors are TypeErrors
            raise ValueError(f'Invalid recording value: {error}') from None

    def trade_ids(self, carry=None):
        """v2 trade IDs: the issue's frame number and the trade's place among that frame's trades. `carry`
        ({issue_id: (frame, place)}, updated) continues the places of a stream read a batch at a time."""
        if getattr(self, '_trade_ids', None) is None:
            ids, key, place = [], None, 0
            for issue, frame in zip(self.t['issue_id'].tolist(), self.t['frame'].tolist()):
                if (issue, frame) == key:
                    place += 1
                elif carry is not None and carry.get(issue, (None,))[0] == frame:
                    place = carry[issue][1] + 1
                else:
                    place = 0
                key = (issue, frame)
                ids.append(f'{frame}-{place}')
                if carry is not None:
                    carry[issue] = (frame, place)
            self._trade_ids = np.array(ids, dtype=object)
        return self._trade_ids

    def _trades(self, midnight, rows):
        t = {k: v[rows] for k, v in self.t.items()}
        codes, ids = self.t_code[rows], self.trade_ids()[rows]
        require(not len(codes) or t['side'].max() < 256, 'Invalid trade side')
        ts = midnight + t['time_us'] * 1000
        aggressor = pa.DictionaryArray.from_arrays(
            pa.array(np.select([t['side'] == 1, t['side'] == 2], [1, 2], 0).astype(np.int8)),
            pa.array(['NO_AGGRESSOR', 'BUY', 'SELL']))
        columns = [_decimal(t['price10'], 1), _decimal(t['quantity'], 0), aggressor, pa.array(ids.tolist(), pa.string()),
                   pa.array(ts, TIME), pa.array(ts, TIME), pa.array([c + '.' + VENUE for c in codes], pa.string()),
                   pa.array(t['issue_id'], pa.uint64()), pa.array(t['seq'], pa.uint64()), pa.array(t['frame'], pa.uint64()),
                   pa.array(t['side'], pa.uint8())]
        return pa.Table.from_arrays(columns, schema=TRADE_SCHEMA)

    def _depths(self, midnight):
        q = self.q
        books = []
        for side in BOOKS:
            offsets, levels = self.books[side]
            require(not len(levels) or levels[:, 2].max() < 2**32, 'Invalid book level')
            items = pa.StructArray.from_arrays(
                [_decimal(levels[:, 0], 1), _decimal(levels[:, 1], 0), pa.array(levels[:, 2], pa.uint32()),
                 pa.array(np.zeros(len(levels), np.uint64))], fields=list(LEVEL))
            books.append(pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), items, type=BOOK))
        ts = midnight + q['source_time_us'] * 1000
        special = q['special_quote_time_us']
        columns = [*books[:2], pa.array(np.where(q['seq'] == 0, F_MBP | F_SNAPSHOT, F_MBP).astype(np.uint8)),
                   pa.array(q['frame'], pa.uint64()), pa.array(ts, TIME), pa.array(ts, TIME),
                   pa.array([c + '.' + VENUE for c in self.code], pa.string()),
                   pa.array(q['issue_id'], pa.uint64()), pa.array(q['seq'], pa.uint64()),
                   *[_decimal(q[k], 1, nulls=True) for k in PRICES],
                   *[pa.array(q[k], pa.uint64()) for k in COUNTS],
                   pa.array(midnight + special * 1000, TIME, mask=special == 0), _decimal(q['turnover10'], 1), *books[2:]]
        return pa.Table.from_arrays(columns, schema=DEPTH_SCHEMA)

    def _equity(self, midnight):
        rows = []
        for m in self.master:
            require(isinstance(m['code'], str) and isinstance(m['name'], str), 'Invalid master entry')
            require(all(type(m[k]) is int and m[k] >= 0 for k in MASTER_KEYS - {'code', 'name'}), 'Invalid master entry')
            rows.append({'id': f"{m['code']}.{VENUE}", 'raw_symbol': m['code'], 'currency': 'JPY', 'price_precision': 1,
                         'price_increment': '0.1', 'lot_size': str(m['lot_size']), 'isin': None, 'max_quantity': None,
                         'min_quantity': None, 'max_price': _price_text(m['limit_up10']),
                         'min_price': _price_text(m['limit_down10']), 'margin_init': '0', 'margin_maint': '0',
                         'tick_scheme': None, 'info': None, 'ts_event': midnight, 'ts_init': midnight,
                         'issue_id': m['issue_id'], 'tick_type': m['tick_type'], 'issue_type': m['issue_type'],
                         'name': m['name'], 'base_price': None})
        table = pa.Table.from_pylist(rows, schema=EQUITY_SCHEMA.remove_metadata())
        base = _decimal([m['base_price10'] for m in self.master], 1, nulls=True)
        return table.set_column(table.schema.get_field_index('base_price'), EQUITY_SCHEMA.field('base_price'), base) \
            .replace_schema_metadata(EQUITY_SCHEMA.metadata)

    def _batches(self, midnight):
        rows = []
        for b in self.batches:
            require(type(b['source_time_us']) is int, 'Invalid batch clock')
            row = {k: None for k in BATCH_SCHEMA.names}
            fields = {k: b[k] for k in BATCH_FIELDS[b['type']]}
            if 'input_transport' in fields:
                fields['input_transport'] = canonical(fields['input_transport'])
            ts = midnight + b['source_time_us'] * 1000
            row.update(fields, seq=b['seq'], kind=b['type'], skipped_issues=b.get(SKIPPED), type='BriskBatch',
                       ts_event=ts, ts_init=ts)
            rows.append(row)
        return pa.Table.from_pylist(rows, schema=BATCH_SCHEMA)

    # Reading

    @classmethod
    def load(cls, directory, issue_id=None):
        """Read a recording's tables (or one issue's rows) into memory, a row group at a time."""
        directory = Path(directory)
        batches = _decode_batches(pq.read_table(directory / BATCHES))
        midnight = midnight_ns(batches[0]['trading_date'])
        master = _decode_equity(pq.read_table(directory / EQUITY))
        if issue_id is None:
            source = pq.ParquetFile(directory / DEPTHS)
            groups = (source.read_row_group(i) for i in range(source.num_row_groups))
        else:
            groups = [pq.read_table(directory / DEPTHS, filters=[('issue_id', '=', issue_id)])]
        parts = {}
        for table in groups:
            for name, values in _decode_depths(table, midnight).items():
                parts.setdefault(name, []).append(values)
        empty = {'code': np.zeros(0, object), **{f'{side}.levels': np.zeros((0, 3), np.int64) for side in BOOKS}}
        merged = {name: np.concatenate(parts.pop(name)) if name in parts else empty.get(name, np.zeros(0, np.int64))
                  for name in (*INTS, 'code', *(f'{side}.{part}' for side in BOOKS for part in ('counts', 'levels')))}
        books = {side: (np.concatenate([[0], np.cumsum(merged.pop(f'{side}.counts'))]), merged.pop(f'{side}.levels'))
                 for side in BOOKS}
        filters = None if issue_id is None else [('issue_id', '=', issue_id)]
        t, t_code = _decode_trades(pq.read_table(directory / TRADES, filters=filters), midnight)
        return cls({k: merged[k] for k in INTS}, merged['code'], books, master, batches, t, t_code)

    def quotes(self, idx) -> list[dict]:
        """Quote dicts (as the decoder emits them) for these rows."""
        idx = np.asarray(idx, dtype=np.int64)
        values = {k: self.q[k][idx].tolist() for k in INTS[1:]}
        values['code'] = self.code[idx].tolist()
        for side, (offsets, levels) in self.books.items():  # noqa: B007
            counts = offsets[1:][idx] - offsets[:-1][idx]
            bounds = np.concatenate([[0], np.cumsum(counts)])
            flat = levels[np.arange(bounds[-1]) + np.repeat(offsets[:-1][idx] - bounds[:-1], counts)].tolist()
            bounds = bounds.tolist()
            values[side] = [flat[bounds[i]:bounds[i + 1]] for i in range(len(idx))]
        return [dict(zip(QUOTE_ORDER, row)) for row in zip(*(values[k] for k in QUOTE_ORDER))]

    def _by_batch(self):
        """The quote and trade rows in batch order, and their batches."""
        order, t_order = np.argsort(self.q['seq'], kind='stable'), np.argsort(self.t['seq'], kind='stable')
        return order, t_order, self.q['seq'][order], self.t['seq'][t_order]

    def iter_batches(self, start=0, stop=None, by_batch=None):
        """The decoder's batches in order (those from `start` to `stop`): quotes sorted by issue, and trades by
        issue then in the decoder's order, as in the canonical stream."""
        order, t_order, seqs, t_seqs = by_batch or self._by_batch()
        batches = self.batches[start:stop]
        at = t_at = 0
        if start and batches:
            at, t_at = (int(np.searchsorted(s, batches[0]['seq'], 'left')) for s in (seqs, t_seqs))
        for b in batches:
            end, t_end = int(np.searchsorted(seqs, b['seq'], 'right')), int(np.searchsorted(t_seqs, b['seq'], 'right'))
            require(int(np.searchsorted(seqs, b['seq'], 'left')) == at
                    and int(np.searchsorted(t_seqs, b['seq'], 'left')) == t_at, 'Quotes or trades outside their batches')
            batch = dict(b)
            if b['type'] == 'bootstrap':
                batch['master'] = self.master
            if b['type'] in ('bootstrap', 'quotes') or end > at:
                batch['quotes'] = self.quotes(order[at:end])
            if b['type'] != 'end' or t_end > t_at:
                batch['trades'] = self.trades_at(t_order[t_at:t_end])
            at, t_at = end, t_end
            yield batch
        if stop is None or stop >= len(self.batches):
            require(at == len(seqs) and t_at == len(t_seqs), 'Quotes or trades outside their batches')

    def summary(self):
        """The archive's summary of this content (validate_batches'), without its checks: the archive runs them
        on every upload. For our own spooled sessions, where checking a day locally would cost minutes."""
        first, last = self.batches[0], self.batches[-1]
        require(first['type'] == 'bootstrap', 'Recording has no bootstrap')
        require(last['type'] == 'end', 'Recording has no clean end')
        n = len(self.batches)
        quotes = np.bincount(self.q['seq'], minlength=n)[:n] if len(self) else np.zeros(n, np.int64)
        trades = np.bincount(self.t['seq'], minlength=n)[:n] if len(self.t_code) else np.zeros(n, np.int64)
        expanded = (batch_lengths(self.batches, self.master, quotes, trades) + int(quote_lengths(self).sum())
                    + int(trade_lengths(self.t, self.t_code).sum()))
        return dict(source=first['source'], trading_date=first['trading_date'], first_source_time_us=first['source_time_us'],
                    last_source_time_us=last['source_time_us'], codes=sorted(m['code'] for m in self.master),
                    batches=n, quote_updates=len(self), expanded_bytes=expanded)

    @classmethod
    def concat(cls, pieces, master):
        """Consecutive pieces of one stream as one: quotes in the canonical order, by issue then batch; trades by
        issue, batch, then the decoder's order (as each piece kept it)."""
        q = {k: np.concatenate([c.q[k] for c in pieces]) for k in INTS}
        code = np.concatenate([c.code for c in pieces])
        books = {}
        for side in BOOKS:
            counts = np.concatenate([np.diff(c.books[side][0]) for c in pieces])
            books[side] = (np.concatenate([[0], np.cumsum(counts)]), np.concatenate([c.books[side][1] for c in pieces]))
        t = {k: np.concatenate([c.t[k] for c in pieces]) for k in TRADE_INTS}
        t_code = np.concatenate([c.t_code for c in pieces])
        t_order = np.lexsort((np.arange(len(t_code)), t['seq'], t['issue_id']))
        return cls(q, code, books, master, [b for c in pieces for b in c.batches], {k: v[t_order] for k, v in t.items()},
                   t_code[t_order], order=np.lexsort((q['seq'], q['issue_id'])))

    def trades_at(self, idx) -> list[dict]:
        """Trade dicts (as the decoder emits them) for these trade rows."""
        values = {k: self.t[k][idx].tolist() for k in TRADE_INTS[1:]}
        values['code'] = self.t_code[idx].tolist()
        keys = ('issue_id', 'code', 'frame', 'time_us', 'price10', 'quantity', 'side')
        return [dict(zip(keys, row)) for row in zip(*(values[k] for k in keys))]

    def trades(self, issue_id=None, low=None, high=None) -> list[dict]:
        """Trades in order (one issue's, or every issue's by issue), within source times [low, high]."""
        ids = self.t['issue_id']
        rows = (np.arange(len(ids)) if issue_id is None else
                np.arange(np.searchsorted(ids, issue_id, 'left'), np.searchsorted(ids, issue_id, 'right')))
        times = self.t['time_us'][rows]
        keep = np.ones(len(rows), bool)
        if low is not None:
            keep &= times >= low
        if high is not None:
            keep &= times <= high
        return self.trades_at(rows[keep])

    @classmethod
    def trades_of(cls, directory):
        """A recording's trades (and batches) alone, without its quotes."""
        directory = Path(directory)
        batches = _decode_batches(pq.read_table(directory / BATCHES))
        master = _decode_equity(pq.read_table(directory / EQUITY))
        t, t_code = _decode_trades(pq.read_table(directory / TRADES), midnight_ns(batches[0]['trading_date']))
        empty = Columns({k: np.zeros(0, np.int64) for k in INTS}, np.zeros(0, dtype=object),
                        {side: (np.zeros(1, np.int64), np.zeros((0, 3), np.int64)) for side in BOOKS}, master, batches, t, t_code)
        return empty

    def state(self, limit=None) -> dict[int, dict]:
        """Latest quote per issue at or before a source time (microseconds)."""
        rows = np.arange(len(self)) if limit is None else np.flatnonzero(self.q['source_time_us'] <= limit)
        issues = self.q['issue_id'][rows]
        last = rows[np.r_[issues[1:] != issues[:-1], True]] if len(rows) else rows
        return {q['issue_id']: q for q in self.quotes(last)}

    def updates(self, issue_id, low=None, high=None) -> list[dict]:
        """One issue's updates in order, within source times [low, high]."""
        ids = self.q['issue_id']
        rows = np.arange(np.searchsorted(ids, issue_id, 'left'), np.searchsorted(ids, issue_id, 'right'))
        times = self.q['source_time_us'][rows]
        keep = np.ones(len(rows), bool)
        if low is not None:
            keep &= times >= low
        if high is not None:
            keep &= times <= high
        return self.quotes(rows[keep])


def _decode_batches(table, midnight=None):
    """The batch rows (master, quotes and trades aside); a recording's start with its bootstrap, or a spool
    part's (whose day starts at `midnight`) anywhere."""
    rows = table.to_pylist()
    require(rows and (midnight is not None or rows[0]['kind'] == 'bootstrap'), 'Recording has no bootstrap')
    times = _numbers(table.column('ts_event')) - (midnight_ns(rows[0]['trading_date']) if midnight is None else midnight)
    require(times.min() >= 0 and not (times % 1000).any(), 'Invalid batch clock')
    out = []
    for row, us in zip(rows, (times // 1000).tolist()):
        require(row['kind'] in BATCH_FIELDS, 'Unknown batch kind')
        b = {'type': row['kind'], 'seq': row['seq'], 'source_time_us': us, **{k: row[k] for k in BATCH_FIELDS[row['kind']]}}
        if row['kind'] == 'bootstrap':
            require(isinstance(row['input_transport'], str), 'Invalid transport fields')
            b['input_transport'] = json.loads(row['input_transport'])
        if row[SKIPPED] is not None:
            b[SKIPPED] = row[SKIPPED]
        out.append(b)
    return out


def _decode_depths(table, midnight):
    """Numpy columns (vendor units) from rows of the depths table."""
    out = {'seq': _numbers(table.column('seq')), 'issue_id': _numbers(table.column('issue_id')),
           'frame': _numbers(table.column('sequence'))}
    ts = _numbers(table.column('ts_event')) - midnight
    require(not len(ts) or (ts.min() >= 0 and not (ts % 1000).any()), 'Invalid quote time')
    out['source_time_us'] = ts // 1000
    special = table.column('special_quote_time')
    times = _numbers(special) - midnight
    times[_nulls(special)] = 0
    require(not len(times) or (times.min() >= 0 and not (times % 1000).any()), 'Invalid special quote time')
    out['special_quote_time_us'] = times // 1000
    out['turnover10'] = _integers(table.column('turnover'), 1)
    for k in PRICES:
        out[k] = _integers(table.column(k[:-2]), 1)
    for k in COUNTS:
        out[k] = _numbers(table.column(k))
    # Codes share one string per security.
    identifiers = table.column('identifier').combine_chunks().dictionary_encode()
    names = identifiers.dictionary.to_pylist()
    suffix = '.' + VENUE
    require(all(name.endswith(suffix) for name in names), 'Invalid identifier')
    out['code'] = np.array([name[:-len(suffix)] for name in names] + [None], dtype=object)[
        identifiers.indices.fill_null(len(names)).to_numpy(zero_copy_only=False)]
    for side, name in BOOKS.items():
        column = table.column(name).combine_chunks()
        bounds = column.offsets.to_numpy().astype(np.int64)
        items = column.flatten()
        require(not len(items) or _numbers(items.field('order_id')).max() == 0, 'Invalid book level')
        out[f'{side}.counts'] = np.diff(bounds)
        out[f'{side}.levels'] = (np.stack([_integers(items.field('price'), 1), _integers(items.field('size'), 0),
                                           _numbers(items.field('count'))], axis=1) if len(items) else np.zeros((0, 3), np.int64))
    return out


def _decode_trades(table, midnight):
    """Trade columns (vendor units) and codes from rows of the trades table."""
    t = {'seq': _numbers(table.column('seq')), 'issue_id': _numbers(table.column('issue_id')),
         'frame': _numbers(table.column('frame')), 'side': _numbers(table.column('side'))}
    ts = _numbers(table.column('ts_event')) - midnight
    require(not len(ts) or (ts.min() >= 0 and not (ts % 1000).any()), 'Invalid trade time')
    t['time_us'] = ts // 1000
    t['price10'] = _integers(table.column('price'), 1)
    t['quantity'] = _integers(table.column('size'), 0)
    identifiers = table.column('identifier').combine_chunks().dictionary_encode()
    names = identifiers.dictionary.to_pylist()
    suffix = '.' + VENUE
    require(all(name.endswith(suffix) for name in names), 'Invalid identifier')
    code = np.array([name[:-len(suffix)] for name in names] + [None], dtype=object)[
        identifiers.indices.fill_null(len(names)).to_numpy(zero_copy_only=False)]
    return {k: t[k] for k in TRADE_INTS}, code


def _decode_equity(table):
    base = _integers(table.column('base_price'), 1).tolist()
    master = []
    for row, base10 in zip(table.to_pylist(), base):
        require(isinstance(row['lot_size'], str) and re.fullmatch(r'\d{1,16}', row['lot_size']), 'Invalid lot size')
        master.append({'issue_id': row['issue_id'], 'code': row['raw_symbol'], 'tick_type': row['tick_type'],
                       'base_price10': base10, 'limit_up10': _tenths(row['max_price']),
                       'limit_down10': _tenths(row['min_price']), 'lot_size': int(row['lot_size']),
                       'issue_type': row['issue_type'], 'name': row['name']})
    return master


# Files and packages

def check(directory, broker=None):
    """Refuse a recording whose files are not exactly the expected tables, before reading their data.

    Beyond the schema: a pyarrow writer, no file metadata but the schema's, full row groups (the last
    may be shorter), zstd throughout and column chunks that fill the file between its header and footer.
    A broker's recording has larger limits: `broker`, or (None) as its own batches say, for one of your own.
    """
    directory = Path(directory)
    if broker is None:
        broker = _source(directory) in BROKER_TRANSPORT
    max_rows, max_decoded = (BROKER_MAX_ROWS, BROKER_MAX_DECODED) if broker else (MAX_ROWS, MAX_DECODED)
    for name in FILES:
        path = directory / name
        require(path.is_file(), f'Missing {name}')
        meta = pq.ParquetFile(path).metadata
        require(meta.schema.to_arrow_schema().equals(SCHEMAS[name], check_metadata=True), f'Unexpected schema in {name}')
        require(re.fullmatch(r'parquet-cpp-arrow version \d{1,4}\.\d{1,4}\.\d{1,4}', meta.created_by or '')
                and set(meta.metadata or {}) == {b'ARROW:schema', *SCHEMAS[name].metadata}
                and all(meta.metadata[k] == v for k, v in SCHEMAS[name].metadata.items()), f'Unexpected file metadata in {name}')
        require(meta.num_rows <= max_rows[name], f'Too many rows in {name}')
        require(all(meta.row_group(i).num_rows == ROW_GROUP for i in range(meta.num_row_groups - 1)), f'Unexpected row groups in {name}')
        require(all(meta.row_group(i).column(j).compression == 'ZSTD' for i in range(meta.num_row_groups)
                    for j in range(meta.num_columns)), f'Unexpected compression in {name}')
        chunks = sorted((c.dictionary_page_offset if c.has_dictionary_page else c.data_page_offset, c.total_compressed_size)
                        for c in (meta.row_group(i).column(j) for i in range(meta.num_row_groups)
                                  for j in range(meta.row_group(i).num_columns)))
        end = 4
        for start, size in chunks:
            require(start == end, f'Unexpected bytes in {name}')
            end = start + size
        require(end == path.stat().st_size - 8 - meta.serialized_size, f'Unexpected bytes in {name}')
        decoded = 0
        for i in range(meta.num_row_groups):
            group = meta.row_group(i)
            decoded += group.total_byte_size
            for c in range(group.num_columns):
                # Book sides hold at most LEVELS entries; skipped issues at most the master.
                limit = (max_rows[EQUITY] if name == BATCHES else LEVELS) * max(group.num_rows, 1)
                require(group.column(c).num_values <= limit, f'Oversized column in {name}')
        require(decoded <= max_decoded, f'{name} exceeds decode limit')


def write(batches, directory):
    """Encode decoder batches (in order) as a recording directory."""
    Columns.from_batches(batches).write(directory)


def stream_tables(batches, carry=None, depths=True, trades=True):
    """(depths, trades) tables of decoder batches, the first a bootstrap (its master may be empty), as a
    recording holds their rows but in arrival order; None for a table not asked for. `carry` continues
    trade IDs across calls (see Columns.trade_ids)."""
    columns = Columns.from_batches(batches)
    midnight = midnight_ns(columns.trading_date)
    if trades:
        columns.trade_ids(carry)
    return (columns._depths(midnight) if depths else None,
            columns._trades(midnight, slice(None)) if trades else None)


def part_columns(lines, header):
    """Start building the Columns of consecutive decoder lines, canonical (canonical_batches): the same as
    Columns.from_batches of them. Returns a function that gives them. Without the GIL they are parsed and built
    from now on, in slices of at least SLICE_LINES lines on threads; with it, by the function, here.
    `header` (the segment's bootstrap without quotes and trades) stands in for the bootstrap of a slice that
    starts later."""
    def build(piece):
        batches = list(canonical_batches(io.BytesIO(b''.join(piece))))
        first = batches[0]['type'] == 'bootstrap'
        columns = Columns.from_batches(batches if first else [header, *batches])
        if not first:
            columns.batches = columns.batches[1:]
        return columns
    pool = _threads()
    if pool is None:
        return lambda: build(lines)
    count = max(1, min(_THREADS, len(lines) // SLICE_LINES))
    step = -(-len(lines) // count)
    pieces = [pool.submit(build, lines[i:i + step]) for i in range(0, len(lines), step)]

    def columns():
        built = [piece.result() for piece in pieces]
        return built[0] if len(built) == 1 else Columns.concat(built, built[0].master)
    return columns


def write_part(columns, header, directory, carry=None):
    """A spool part (see briskapi._spool): the depths, trades and batch tables of consecutive decoder lines'
    Columns (part_columns).

    `header` is the segment's bootstrap without quotes and trades; it stands in for the master when the part
    starts later. `carry` continues trade IDs from the parts before. Equity is written once, by the spool.
    """
    midnight = midnight_ns(header['trading_date'])
    columns.trade_ids(carry)
    directory = Path(directory)
    (directory / BATCHES).parent.mkdir(parents=True, exist_ok=True)
    try:
        pq.write_table(columns._batches(midnight), directory / BATCHES, row_group_size=ROW_GROUP, **WRITE)
        rows = columns.sorted()
        issues = rows.q['issue_id']
        with pq.ParquetWriter(directory / DEPTHS, DEPTH_SCHEMA, **WRITE) as writer:
            # A row group per PART_ISSUES issue IDs, so that a piece of the day is read a range of issues at a time.
            starts = np.flatnonzero(np.diff(issues // PART_ISSUES, prepend=-1)) if len(issues) else np.zeros(1, np.int64)
            for start, stop in zip(starts.tolist(), [*starts[1:].tolist(), len(issues)]):
                writer.write_table(rows.take(np.arange(start, stop))._depths(midnight), row_group_size=ROW_GROUP)
        pq.write_table(columns._trades(midnight, slice(None)), directory / TRADES, row_group_size=ROW_GROUP, **WRITE)
    except (TypeError, OverflowError) as error:
        raise ValueError(f'Invalid recording value: {error}') from None


def _part_columns(part, midnight, master):
    """A spool part's rows as Columns, in the part's order."""
    batches = _decode_batches(pq.read_table(part / BATCHES), midnight)
    q = _decode_depths(pq.read_table(part / DEPTHS), midnight)
    books = {side: (np.concatenate([[0], np.cumsum(q.pop(f'{side}.counts'))]), q.pop(f'{side}.levels')) for side in BOOKS}
    t, t_code = _decode_trades(pq.read_table(part / TRADES), midnight)
    return Columns({k: q[k] for k in INTS}, q['code'], books, master, batches, t, t_code)


def load_parts(equity, parts, midnight=None):
    """The canonical Columns of a spooled segment: its equity table and its parts, in order. Written, they are
    the same bytes as the segment's batches written at once (Columns.from_batches(...).write). Parts that do not
    start with the bootstrap need its day's `midnight`."""
    master = _decode_equity(pq.read_table(equity))
    pieces = []
    for part in parts:
        if midnight is None:
            first = _decode_batches(pq.read_table(Path(part) / BATCHES))[0]
            midnight = midnight_ns(first['trading_date'])
        pieces.append(_part_columns(Path(part), midnight, master))
    require(pieces, 'Recording has no bootstrap')
    return Columns.concat(pieces, master)


# Sizes in canonical JSON (schema.encode), as the archive's summary counts them, from the columns: a quote's
# canonical object is its keys in order with their values, so its length is the keys' and punctuation's, which
# are fixed, plus its numbers' digits, its code's and its books'.

_POWERS = 10 ** np.arange(1, 19, dtype=np.int64)


def _digits(values):
    """len(str(v)) for each integer."""
    values = np.asarray(values, dtype=np.int64)
    return 1 + np.searchsorted(_POWERS, np.abs(values), side='right') + (values < 0)


def _object_length(keys):
    """'{', '}', a ':' and two quotes per key, the keys and the commas between them."""
    return 2 + sum(len(k) + 3 for k in keys) + len(keys) - 1


_QUOTE_FIXED = _object_length(QUOTE_KEYS)
_TRADE_FIXED = _object_length(TRADE_KEYS)


def quote_lengths(columns):
    """Each quote row's canonical JSON length (schema.canonical of the dict Columns.quotes makes)."""
    n = len(columns)
    total = np.full(n, _QUOTE_FIXED, np.int64)
    for k in INTS[1:]:
        total += _digits(columns.q[k])
    total += np.fromiter((len(c) + 2 for c in columns.code), np.int64, n)   # codes need no escapes
    for offsets, levels in columns.books.values():
        counts = np.diff(offsets)
        per_level = (_digits(levels).sum(axis=1) + 4) if len(levels) else np.zeros(0, np.int64)   # '[', ']', two ','
        running = np.concatenate([[0], np.cumsum(per_level)])
        total += 2 + running[offsets[1:]] - running[offsets[:-1]] + np.maximum(counts - 1, 0)
    return total


def trade_lengths(t, t_code):
    """Each trade row's canonical JSON length."""
    n = len(t_code)
    total = np.full(n, _TRADE_FIXED, np.int64)
    for k in TRADE_INTS[1:]:
        total += _digits(t[k])
    return total + np.fromiter((len(c) + 2 for c in t_code), np.int64, n)


def batch_lengths(batches, master, quotes, trades):
    """The batches' canonical lines (schema.encode, as Columns.iter_batches makes them) but for their quotes' and
    trades' own objects: each with its master (the bootstrap), and its lists' brackets and commas (`quotes` and
    `trades`: how many each batch holds)."""
    total = 0
    for b, nq, nt in zip(batches, quotes.tolist(), trades.tolist()):
        skeleton = dict(b)
        if b['type'] == 'bootstrap':
            skeleton['master'] = master
        if b['type'] in ('bootstrap', 'quotes') or nq:
            skeleton['quotes'] = []
            total += max(nq - 1, 0)
        if b['type'] != 'end' or nt:
            skeleton['trades'] = []
            total += max(nt - 1, 0)
        total += len(encode(skeleton))
    return total


# Segments: a day's spool shipped a few minutes at a time (briskapi._spool), each piece a recording of its own

def _no_trades():
    return {k: np.zeros(0, np.int64) for k in TRADE_INTS}, np.zeros(0, dtype=object)


def latest(columns, before=None):
    """Each issue's last quote row in `columns`, or in `before` (the same, for what came earlier) for an issue that
    has none there: the book as it stands after them."""
    rows = [c for c in (before, columns) if c is not None and len(c)]
    if not rows:
        return before
    t, t_code = _no_trades()
    both = Columns.concat([Columns(c.q, c.code, c.books, c.master, [], t, t_code) for c in rows], columns.master)
    order = both.order
    issues = both.q['issue_id'][order]
    last = np.ones(len(order), bool)
    last[:-1] = issues[1:] != issues[:-1]
    out = both.take(order[last])
    out.batches, out.order = [], None
    return out


def after(columns, cut, bootstrap):
    """Consecutive batches of a day (`columns`, from a spool's parts) as a recording of its own: a bootstrap like
    the day's (`bootstrap`, its batch row) at the clock of the batch before them, holding each issue's quote as
    of then (`cut.latest`; the issues with none skipped), then the batches, numbered on from it."""
    shift = columns.batches[0]['seq'] - 1
    head = {k: v for k, v in bootstrap.items() if k != SKIPPED} | {'seq': 0, 'source_time_us': cut.clock}
    quoted = set(cut.latest.q['issue_id'].tolist()) if cut.latest is not None else set()
    skipped = sorted(m['issue_id'] for m in columns.master if m['issue_id'] not in quoted)
    if skipped:
        head[SKIPPED] = skipped
    t, t_code = _no_trades()
    pieces = [Columns({**columns.q, 'seq': columns.q['seq'] - shift}, columns.code, columns.books, columns.master,
                      [dict(b, seq=b['seq'] - shift) for b in columns.batches], {**columns.t, 'seq': columns.t['seq'] - shift},
                      columns.t_code)]
    if cut.latest is not None:
        state = cut.latest
        pieces.insert(0, Columns({**state.q, 'seq': np.zeros(len(state), np.int64)}, state.code, state.books,
                                 columns.master, [head], t, t_code))
    else:
        pieces.insert(0, Columns({k: np.zeros(0, np.int64) for k in INTS}, np.zeros(0, dtype=object),
                                 {side: (np.zeros(1, np.int64), np.zeros((0, 3), np.int64)) for side in BOOKS},
                                 columns.master, [head], t, t_code))
    return Columns.concat(pieces, columns.master)


def recount_end(columns):
    """A segment's end counts its own quote updates (the host's counts the whole day's)."""
    columns.batches[-1] = dict(columns.batches[-1], quote_updates=_updates(columns.batches,
                                                                         _per_batch(columns.q['seq'], len(columns.batches))))


def _rows(columns, idx):
    """These quote rows alone (no batches or trades), in this order."""
    out = columns.take(idx)
    out.batches, out.order = [], None
    out.t, out.t_code = _no_trades()
    return out


def _depth_rows(table, midnight, master):
    """Rows of a depths table as Columns of quotes alone."""
    q = _decode_depths(table, midnight)
    books = {side: (np.concatenate([[0], np.cumsum(q.pop(f'{side}.counts'))]), q.pop(f'{side}.levels')) for side in BOOKS}
    t, t_code = _no_trades()
    return Columns({k: q[k] for k in INTS}, q['code'], books, master, [], t, t_code)


def _join(pieces):
    """Quote rows (Columns of quotes alone) one after another, as one."""
    if len(pieces) == 1:
        return pieces[0]
    return Columns({k: np.concatenate([r.q[k] for r in pieces]) for k in INTS}, np.concatenate([r.code for r in pieces]),
                   {side: (np.concatenate([[0], np.cumsum(np.concatenate([np.diff(r.books[side][0]) for r in pieces]))]),
                           np.concatenate([r.books[side][1] for r in pieces])) for side in BOOKS},
                   pieces[0].master, [], *_no_trades())


class _DepthWriter:
    """Writes sorted quote rows given a slice at a time in the row groups Columns.write makes: ROW_GROUP rows each,
    the last shorter."""

    def __init__(self, path, midnight):
        self._writer, self._midnight, self._pending, self._count = pq.ParquetWriter(path, DEPTH_SCHEMA, **WRITE), midnight, [], 0

    def add(self, rows):
        if len(rows):
            self._pending.append(rows)
            self._count += len(rows)
        while self._count >= ROW_GROUP:
            self._flush(ROW_GROUP)

    def _flush(self, count):
        joined = _join(self._pending)
        self._writer.write_table(_rows(joined, np.arange(count))._depths(self._midnight), row_group_size=ROW_GROUP)
        self._pending = [_rows(joined, np.arange(count, len(joined)))] if count < len(joined) else []
        self._count -= count

    def close(self):
        if self._count:
            self._flush(self._count)
        self._writer.close()


def _ranges(parts):
    """Issue ID ranges [low, high) to read the parts' quotes by, each about PIECE_ROWS rows across the parts (from
    their depths row groups' statistics: write_part gives each PART_ISSUES issue IDs their own)."""
    blocks = {}
    for part in parts:
        meta = pq.ParquetFile(Path(part) / DEPTHS).metadata
        column = next(c for c in range(meta.num_columns) if meta.schema.column(c).path == 'issue_id') if meta.num_row_groups else None
        for g in range(meta.num_row_groups):
            stats = meta.row_group(g).column(column).statistics
            block = stats.min // PART_ISSUES if stats is not None and stats.has_min_max else 0
            blocks[block] = blocks.get(block, 0) + meta.row_group(g).num_rows
    ranges, low, rows = [], 0, 0
    for block in sorted(blocks):
        rows += blocks[block]
        if rows >= PIECE_ROWS:
            ranges.append((low, (block + 1) * PART_ISSUES))
            low, rows = (block + 1) * PART_ISSUES, 0
    return [*ranges, (low, 2**63 - 1)]


def write_piece(equity, parts, directory, midnight, cut=None, bootstrap=None, end=None):
    """Write consecutive spooled parts as one recording in `directory`, a range of issues from every part at a
    time, so that no day is ever whole in memory: the same tables as their Columns written at once (load_parts,
    then `after` a cut, then their end, then Columns.write), which tests/test_spool.py checks.

    `cut` and `bootstrap` (the day's bootstrap row): a piece that starts after a cut (see after()). `end`: None for
    the parts' own end, 'repair' for one if they have none (repair_end), 'cut' for one that ends a piece.
    Returns (summary, each issue's last quote as Columns, the last batch's clock, whether an end was repaired)."""
    directory = Path(directory)
    for name in FILES:
        (directory / name).parent.mkdir(parents=True, exist_ok=True)
    master = _decode_equity(pq.read_table(equity))
    rows = [b for part in parts for b in _decode_batches(pq.read_table(Path(part) / BATCHES), midnight)]
    require(rows, 'Recording has no bootstrap')
    first_seq = rows[0]['seq']
    counts = [np.zeros(len(rows), np.int64), np.zeros(len(rows), np.int64)]
    for part in parts:
        for i, name in enumerate((DEPTHS, TRADES)):
            seqs = _numbers(pq.read_table(Path(part) / name, columns=['seq']).column('seq')) - first_seq
            counts[i] += _per_batch(seqs, len(rows))
    shift, head = 0, None
    if cut is not None:
        shift = first_seq - 1
        head = {k: v for k, v in bootstrap.items() if k != SKIPPED} | {'seq': 0, 'source_time_us': cut.clock}
        quoted = set(cut.latest.q['issue_id'].tolist()) if cut.latest is not None else set()
        skipped = sorted(m['issue_id'] for m in master if m['issue_id'] not in quoted)
        if skipped:
            head[SKIPPED] = skipped
        rows = [head, *(dict(b, seq=b['seq'] - shift) for b in rows)]
        counts = [np.concatenate([[len(quoted)], counts[0]]), np.concatenate([[0], counts[1]])]
    else:
        require(rows[0]['type'] == 'bootstrap', 'Recording has no bootstrap')
    repaired = False
    if end == 'cut' or (end == 'repair' and rows[-1]['type'] != 'end'):
        rows.append(_end(rows, counts[0]))
        counts = [np.append(c, 0) for c in counts]
        repaired = end == 'repair'
    elif cut is not None and rows[-1]['type'] == 'end':
        rows[-1] = dict(rows[-1], quote_updates=_updates(rows, counts[0]))
    t, t_code = _no_trades()
    shell = Columns(None, None, None, master, rows, t, t_code)
    try:
        pq.write_table(shell._equity(midnight), directory / EQUITY, row_group_size=ROW_GROUP, **WRITE)
        pq.write_table(shell._batches(midnight), directory / BATCHES, row_group_size=ROW_GROUP, **WRITE)
        depths, latest, quote_bytes, quote_rows = _DepthWriter(directory / DEPTHS, midnight), [], 0, 0
        for low, high in _ranges(parts):
            pieces = []
            for part in parts:
                source = pq.ParquetFile(Path(part) / DEPTHS)
                meta = source.metadata
                column = next((c for c in range(meta.num_columns) if meta.schema.column(c).path == 'issue_id'), None)
                groups = [g for g in range(meta.num_row_groups)
                          if not ((stats := meta.row_group(g).column(column).statistics) is not None and stats.has_min_max
                                  and (stats.max < low or stats.min >= high))]
                if groups:
                    part_rows = _depth_rows(source.read_row_groups(groups), midnight, master)
                    issue = part_rows.q['issue_id']
                    part_rows = _rows(part_rows, np.flatnonzero((issue >= low) & (issue < high)))
                    part_rows.q['seq'] = part_rows.q['seq'] - shift
                    pieces.append(part_rows)
            if cut is not None and cut.latest is not None:
                issue = cut.latest.q['issue_id']
                state = _rows(cut.latest, np.flatnonzero((issue >= low) & (issue < high)))
                state.q['seq'] = np.zeros(len(state), np.int64)
                pieces.insert(0, state)
            if not pieces:
                continue
            joined = Columns.concat([Columns(r.q, r.code, r.books, master, [], *_no_trades()) for r in pieces], master)
            ordered = _rows(joined, joined.order)
            if not len(ordered):
                continue
            depths.add(ordered)
            quote_bytes += int(quote_lengths(ordered).sum())
            quote_rows += len(ordered)
            issues = ordered.q['issue_id']
            last = np.ones(len(issues), bool)
            last[:-1] = issues[1:] != issues[:-1]
            latest.append(_rows(ordered, np.flatnonzero(last)))
        depths.close()
        trades = [_decode_trades(pq.read_table(Path(part) / TRADES), midnight) for part in parts]
        t = {k: np.concatenate([x[0][k] for x in trades]) for k in TRADE_INTS} if trades else _no_trades()[0]
        t_code = np.concatenate([x[1] for x in trades]) if trades else _no_trades()[1]
        t['seq'] = t['seq'] - shift
        order = np.lexsort((np.arange(len(t_code)), t['seq'], t['issue_id']))
        trade_rows = Columns(None, None, None, master, rows, {k: v[order] for k, v in t.items()}, t_code[order])
        with pq.ParquetWriter(directory / TRADES, TRADE_SCHEMA, **WRITE) as writer:
            for start in range(0, len(trade_rows.t_code), ROW_GROUP):
                writer.write_table(trade_rows._trades(midnight, slice(start, start + ROW_GROUP)), row_group_size=ROW_GROUP)
    except (TypeError, OverflowError) as error:
        raise ValueError(f'Invalid recording value: {error}') from None
    require(rows[-1]['type'] == 'end', 'Recording has no clean end')
    expanded = (batch_lengths(rows, master, counts[0], counts[1]) + quote_bytes
                + int(trade_lengths(trade_rows.t, trade_rows.t_code).sum()))
    summary = dict(source=rows[0]['source'], trading_date=rows[0]['trading_date'], first_source_time_us=rows[0]['source_time_us'],
                   last_source_time_us=rows[-1]['source_time_us'], codes=sorted(m['code'] for m in master),
                   batches=len(rows), quote_updates=quote_rows, expanded_bytes=expanded)
    state = None
    if latest:
        state = Columns.concat([Columns(r.q, r.code, r.books, master, [], *_no_trades()) for r in latest], master)
        state = _rows(state, state.order)
    return summary, state, rows[-1]['source_time_us'], repaired


def state(directory, limit=None) -> dict[int, dict]:
    """Columns.state of a recording read a row group at a time, never whole in memory: the latest quote per issue
    at or before a source time (microseconds)."""
    directory = Path(directory)
    batches = _decode_batches(pq.read_table(directory / BATCHES, columns=None))
    midnight, master = midnight_ns(batches[0]['trading_date']), _decode_equity(pq.read_table(directory / EQUITY))
    source, out = pq.ParquetFile(directory / DEPTHS), {}
    for g in range(source.num_row_groups):
        out.update(_depth_rows(source.read_row_group(g), midnight, master).state(limit))   # an issue's later rows win
    return out


def bootstrap_row(part):
    """The batch row of the bootstrap that starts a spool's first part."""
    first = _decode_batches(pq.read_table(Path(part) / BATCHES))[0]
    require(first['type'] == 'bootstrap', 'Recording has no bootstrap')
    return first


def part_index(part, midnight):
    """What choosing where to cut a spool needs of a part, from its batch table and metadata: its quote rows,
    whether it holds the end, and its first and last batch's clock."""
    part = Path(part)
    batches = pq.read_table(part / BATCHES, columns=['kind', 'ts_event'])
    clocks = (_numbers(batches.column('ts_event')) - midnight) // 1000
    return {'rows': pq.ParquetFile(part / DEPTHS).metadata.num_rows, 'end': batches.column('kind')[-1].as_py() == 'end',
            'first': int(clocks[0]), 'last': int(clocks[-1])}


def repair_end(columns) -> bool:
    """Give a segment cut short (a crash, a forced kill) the end its host never wrote, so it is a recording like
    any other; True if it needed one. The end counts the segment's quote updates exactly, as the host would; the
    host's frame count and wall time are gone with it, so `frames` is the least a recording allows (one per batch
    after the bootstrap) and `replay_wall_ms` the span of the batches' local receipt times."""
    if columns.batches[-1]['type'] == 'end':
        return False
    columns.batches = [*columns.batches, _end(columns.batches, _per_batch(columns.q['seq'], len(columns.batches)))]
    return True


def _per_batch(seqs, n):
    return np.bincount(seqs, minlength=n)[:n] if len(seqs) else np.zeros(n, np.int64)


def _updates(batches, quotes):
    """The quote updates of the 'quotes' batches (`quotes`: each batch's quote count), as an end counts them."""
    return int(sum(n for b, n in zip(batches, quotes.tolist()) if b['type'] == 'quotes'))


def _end(batches, quotes):
    """The end a recording cut short gets (repair_end)."""
    last = batches[-1]
    received = [b['received_unix_ms'] for b in batches if b['type'] == 'quotes' and b.get('received_unix_ms') is not None]
    return {'type': 'end', 'seq': last['seq'] + 1, 'source_time_us': last['source_time_us'], 'frames': last['seq'],
            'quote_updates': _updates(batches, quotes), 'replay_wall_ms': float(received[-1] - received[0]) if received else 0.0}


def part_batches(part, header, master):
    """A spool part's decoder batches, in order (quotes by issue within a batch, as the canonical stream has them)."""
    return _part_columns(Path(part), midnight_ns(header['trading_date']), master).iter_batches()


def equity_table(bootstrap):
    """The equity table of a bootstrap batch's master."""
    master = sorted(bootstrap['master'], key=lambda m: m['issue_id'])
    return Columns(None, None, None, master, [bootstrap], None, None)._equity(midnight_ns(bootstrap['trading_date']))


def read(directory):
    """A recording directory's batches, in order."""
    check(directory)
    return Columns.load(directory).iter_batches()


def header(directory):
    """(bootstrap batch without quotes, last batch clock) from the small tables only."""
    directory = Path(directory)
    batches = _decode_batches(pq.read_table(directory / BATCHES))
    bootstrap = dict(batches[0], master=_decode_equity(pq.read_table(directory / EQUITY)))
    return bootstrap, batches[-1]['source_time_us']


def frames(directory):
    """Every batch without master, quotes and trades, with how many quotes and trades it held."""
    directory = Path(directory)
    batches = _decode_batches(pq.read_table(directory / BATCHES))
    counts = {name: np.bincount(_numbers(pq.read_table(directory / table, columns=['seq']).column('seq')),
                                minlength=len(batches)) for name, table in (('quote_count', DEPTHS), ('trade_count', TRADES))}
    return [{**b, **{name: int(c[i]) if i < len(c) else 0 for name, c in counts.items()}} for i, b in enumerate(batches)]


def pack(directory, names=FILES) -> bytes:
    """The package: a stored zip of the tables (`names`, all by default), byte-identical for identical tables."""
    directory = Path(directory)
    out = io.BytesIO()
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_STORED) as z:
        for name in names:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system, info.external_attr = 3, 0o644 << 16
            z.writestr(info, (directory / name).read_bytes())
    return out.getvalue()


def pack_to(directory, target, names=FILES):
    """The package (pack's bytes) written to the file `target` a megabyte at a time; returns its (sha256, size)."""
    directory, target = Path(directory), Path(target)
    with zipfile.ZipFile(target, 'w', zipfile.ZIP_STORED) as z:
        for name in names:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system, info.external_attr = 3, 0o644 << 16
            info.file_size = (directory / name).stat().st_size   # as writestr knows it: the same header
            with (directory / name).open('rb') as source, z.open(info, 'w') as sink:
                shutil.copyfileobj(source, sink, 2**20)
    return file_digest(target), target.stat().st_size


def file_digest(path) -> str:
    with open(path, 'rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def unpack_file(path, directory, limit, names=FILES):
    """Extract a package file's tables (`names`, at most `limit` bytes), refusing anything but exactly them."""
    directory = Path(directory)
    with zipfile.ZipFile(path) as z:
        infos = z.infolist()
        require([i.filename for i in infos] == list(names) and all(i.compress_type == zipfile.ZIP_STORED for i in infos)
                and sum(i.file_size for i in infos) <= limit, 'Unexpected package contents')
        for name, info in zip(names, infos):
            (directory / name).parent.mkdir(parents=True, exist_ok=True)
            with z.open(info) as source, (directory / name).open('wb') as sink:
                shutil.copyfileobj(source, sink, 2**20)


def unpack(data: bytes, directory, names=FILES):
    """Extract a package's tables, refusing anything but exactly the stored tables (`names`)."""
    directory = Path(directory)
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        infos = z.infolist()
        require([i.filename for i in infos] == list(names) and all(i.compress_type == zipfile.ZIP_STORED for i in infos)
                and sum(i.file_size for i in infos) <= MAX_COMPRESSED, 'Unexpected package contents')
        for name, info in zip(names, infos):
            (directory / name).parent.mkdir(parents=True, exist_ok=True)
            (directory / name).write_bytes(z.read(info))


def summarize(directory, references=None):
    """Check and validate a recording directory; returns the archive summary. A broker's is checked a row group
    at a time (verify_broker), anything else whole."""
    if _source(directory) in BROKER_TRANSPORT:
        return verify_broker(directory)
    check(directory)
    return validate_batches(Columns.load(directory).iter_batches(), references)


def _source(directory):
    try:
        return pq.read_table(Path(directory) / BATCHES, columns=['source']).column(0)[0].as_py()
    except (OSError, IndexError, KeyError, pa.ArrowException):
        return None


def verify_package(directory, manifest, references=None):
    """The archive's checks of an upload's tables (all four, in `directory`) against its manifest: a broker's a
    row group at a time (verify_broker), a demo or synthetic one whole (verify_directory)."""
    if validate_manifest(manifest)['summary']['source'] in BROKER_TRANSPORT:
        return verify_broker(directory, manifest)
    return verify_directory(directory, manifest, references)


def verify_tables(directory, columns):
    """Refuse tables that are not the canonical encoding of their decoded content.

    Each row group is compared with the encoding the validated content implies, so columns the
    decoder does not read (derived timestamps, flags, v2's constant fields) and the row order are
    checked as well. Nothing is written.
    """
    directory = Path(directory)
    midnight = midnight_ns(columns.trading_date)
    keys = np.stack([columns.q['issue_id'], columns.q['seq']], axis=1)
    require(not len(keys) or ((keys[1:, 0] > keys[:-1, 0]) | ((keys[1:, 0] == keys[:-1, 0]) & (keys[1:, 1] > keys[:-1, 1]))).all(),
            'Quotes not sorted by issue, then batch')
    keys = np.stack([columns.t['issue_id'], columns.t['seq']], axis=1)
    require(not len(keys) or ((keys[1:, 0] > keys[:-1, 0]) | ((keys[1:, 0] == keys[:-1, 0]) & (keys[1:, 1] >= keys[:-1, 1]))).all(),
            'Trades not sorted by issue, then batch')
    for name, expected in ((EQUITY, columns._equity(midnight)), (BATCHES, columns._batches(midnight))):
        require(pq.read_table(directory / name).equals(expected), f'Non-canonical {name}')
    source = pq.ParquetFile(directory / DEPTHS)
    at = 0
    for i in range(source.num_row_groups):
        group = source.read_row_group(i)
        rows = columns.take(np.arange(at, at + len(group)))
        require(group.equals(rows._depths(midnight)), f'Non-canonical {DEPTHS}')
        at += len(group)
    require(at == len(columns), f'Non-canonical {DEPTHS}')
    # Parquet rebuilds dictionaries from its pages, so aggressor sides compare as their strings.
    plain = lambda table: table.set_column(2, 'aggressor_side', table.column(2).cast(pa.string()))  # noqa: E731
    source = pq.ParquetFile(directory / TRADES)
    at = 0
    for i in range(source.num_row_groups):
        group = source.read_row_group(i)
        require(plain(group).equals(plain(columns._trades(midnight, slice(at, at + len(group))))), f'Non-canonical {TRADES}')
        at += len(group)
    require(at == len(columns.t_code), f'Non-canonical {TRADES}')


# A broker's recording, checked a row group at a time

def _check_master(master):
    """The master as schema's checks take it (see schema._scan): unique, sorted identities and valid entries."""
    require(isinstance(master, list) and 0 < len(master) <= 20000, 'Invalid master size')
    issue = -1
    for m in master:
        require(isinstance(m, dict) and set(m) == MASTER_KEYS, 'Unknown or missing master fields')
        previous, issue = issue, integer(m['issue_id'], 2**32 - 1)
        require(issue > previous, 'Master identities must be unique and sorted')
        code(m['code'])
        for key, value in m.items():
            if key == 'name':
                require(isinstance(value, str) and len(value) <= 256, 'Invalid security name')
            elif key != 'code':
                integer(value)


def _check_batches(batches, master):
    """The batch rules of schema._scan, for a broker's recording: its batches without their quotes and trades.
    Returns each batch's kind and clock."""
    require(batches and len(batches) <= BROKER_MAX_ROWS[BATCHES], 'Recording has no bootstrap')
    identities = {m['issue_id'] for m in master}
    first, last, received, previous_stamp, previous_kind = batches[0], 0, None, None, None
    require(first['type'] == 'bootstrap' and first['source'] in BROKER_TRANSPORT, 'Unsupported bootstrap/source')
    require(isinstance(first['trading_date'], str) and re.fullmatch(r'\d{8}', first['trading_date']), 'Invalid date')
    dt.datetime.strptime(first['trading_date'], '%Y%m%d')
    require(integer(first['market_issue_count']) >= len(master), 'Invalid market count')
    require(first['input_transport'] == BROKER_TRANSPORT[first['source']], 'Invalid broker transport fields')
    require(first['source_timestamp_origin'] == 'brisk_decoder_unverified', 'Invalid clock provenance')
    require(first['exchange_delay_ms'] is None, 'Unverified exchange delay')
    for seq, b in enumerate(batches):
        kind = b['type']
        require(integer(b['seq']) == seq, 'Sequence gap')
        now = integer(b['source_time_us'], 86_400_000_000 - 1)
        require(now >= last, 'Market clock regression')
        require(seq == 0 or kind != 'bootstrap', 'Expected delta/end')
        if kind == 'history':
            require(previous_kind in {'bootstrap', 'history'} and now == last, 'History out of place')
        elif seq:
            require(kind in {'quotes', 'end'}, 'Expected delta/end')
        require(kind != 'end' or seq == len(batches) - 1, 'Data after end batch')
        if kind == 'quotes':
            integer(b['decode_ns'], MAX_DECODE_NS)
            stamp = integer(b['received_unix_ms'])
            require(UNIX_MS[0] <= stamp < UNIX_MS[1], 'Implausible local receipt clock')
            if received is None:
                received = previous_stamp = stamp
            require(previous_stamp <= stamp <= received + MAX_SESSION_MS, 'Local receipt clock regression/span')
            previous_stamp = stamp
            require(b['replay_lateness_ms'] is None, 'Broker streams are not demo replays')
        if SKIPPED in b:
            skipped = b[SKIPPED]
            require(kind in ('bootstrap', 'quotes') and isinstance(skipped, list) and skipped and
                    all(issue in identities for issue in skipped) and skipped == sorted(set(skipped)), 'Invalid skipped issues')
        if kind == 'end':
            require(integer(b['frames']) >= seq - 1, 'End summary mismatch')
            integer(b['quote_updates'])
            measurement(b['replay_wall_ms'], MAX_SESSION_MS)
        last, previous_kind = now, kind
    require(batches[-1]['type'] == 'end', 'Recording has no clean end')
    kinds = np.array([b['type'] for b in batches])
    return kinds, np.array([b['source_time_us'] for b in batches], np.int64)


def _monotonic(issues, values, carry):
    """Each issue's values do not go back (rows sorted by issue), from `carry` ({issue: last value}, updated)."""
    if not len(issues):
        return True
    same = issues[1:] == issues[:-1]
    ok = not (same & (values[1:] < values[:-1])).any()
    first = int(issues[0])
    ok = ok and (first not in carry or values[0] >= carry[first])
    ends = np.flatnonzero(np.append(~same, True))
    carry.update(zip(issues[ends].tolist(), values[ends].tolist()))
    return ok


def verify_broker(directory, manifest=None):
    """Verify a broker's recording (all four tables, in `directory`) a row group at a time, never whole in memory:
    exactly the expected tables, each row group the canonical encoding of its content (so nothing but the
    recording's fields can be in it), the batches' rules (schema._scan's: sequence, kinds, clocks, transport,
    local timing), and of every quote and trade its identity, order, batch, clock and value ranges, and its
    books' order. Not checked, the market's own consistency (a broker's content is uploader-declared: trades
    against volumes, each issue's frames over time). Returns the summary, which must be the manifest's."""
    directory = Path(directory)
    if manifest is not None:
        validate_manifest(manifest)
    check(directory, broker=True)
    batches = _decode_batches(pq.read_table(directory / BATCHES))
    midnight = midnight_ns(batches[0]['trading_date'])
    master = _decode_equity(pq.read_table(directory / EQUITY))
    _check_master(master)
    kinds, clocks = _check_batches(batches, master)
    shell = Columns(None, None, None, master, batches, *_no_trades())
    for name, expected in ((EQUITY, shell._equity(midnight)), (BATCHES, shell._batches(midnight))):
        require(pq.read_table(directory / name).equals(expected), f'Non-canonical {name}')
    ids = np.array([m['issue_id'] for m in master], np.int64)
    codes = np.array([m['code'] for m in master], dtype=object)
    skipped = np.array(sorted(i << 32 | b['seq'] for b in batches for i in b.get(SKIPPED, ())), np.int64)
    n = len(batches)
    quotes, trades = np.zeros(n, np.int64), np.zeros(n, np.int64)
    quote_bytes = trade_bytes = 0
    last_key, frames, times = (-1, -1), {}, {}

    def identity(issues, row_codes):
        at = np.searchsorted(ids, issues)
        require(not len(issues) or ((at < len(ids)) & (ids[np.minimum(at, len(ids) - 1)] == issues)).all(), 'Unknown issue')
        require(not len(issues) or (codes[at] == row_codes).all(), 'Quote identity mismatch/duplicate')

    def batch_of(seqs, allowed, times_us, what):
        require(not len(seqs) or (seqs.max() < n and np.isin(kinds[seqs], allowed).all()), f'{what} outside their batches')
        require(not len(seqs) or (times_us <= clocks[seqs]).all(), f'{what} after their batch clock')

    source = pq.ParquetFile(directory / DEPTHS)
    for g in range(source.num_row_groups):
        table = source.read_row_group(g)
        rows = _depth_rows(table, midnight, master)
        require(table.equals(rows._depths(midnight)), f'Non-canonical {DEPTHS}')
        issue, seq = rows.q['issue_id'], rows.q['seq']
        identity(issue, rows.code)
        keys = issue << 32 | seq
        require(not len(keys) or ((np.diff(keys) > 0).all() and keys[0] > (last_key[0] << 32 | last_key[1])),
                'Quotes not sorted by issue, then batch')
        if len(keys):
            last_key = (int(issue[-1]), int(seq[-1]))
        batch_of(seq, ['bootstrap', 'quotes'], rows.q['source_time_us'], 'Quotes')
        require(not len(skipped) or not np.isin(keys, skipped).any(), 'Invalid skipped issues')
        require(all(((v >= 0) & (v < 2**53)).all() for v in rows.q.values()) and (rows.q['frame'] < 2**32).all()
                and (issue < 2**32).all(), 'Invalid unsigned integer')
        require(_monotonic(issue, rows.q['frame'], frames) and _monotonic(issue, rows.q['source_time_us'], times),
                'Quote frame/time regression')
        for side, (offsets, levels) in rows.books.items():
            require(not len(offsets) or np.diff(offsets).max(initial=0) <= LEVELS, 'Invalid book side')
            require(((levels >= 0) & (levels < 2**53)).all(), 'Invalid unsigned integer')
            if len(levels) > 1:
                within = np.ones(len(levels) - 1, bool)
                within[(offsets[1:-1] - 1)[(offsets[1:-1] > 0) & (offsets[1:-1] < len(levels))]] = False
                step = np.diff(levels[:, 0])
                order = step < 0 if side.startswith('bid') else step > 0
                require(order[within].all(), 'Book prices out of order')
        quotes += _per_batch(seq, n)
        quote_bytes += int(quote_lengths(rows).sum())
    plain = lambda table: table.set_column(2, 'aggressor_side', table.column(2).cast(pa.string()))  # noqa: E731
    source, carry, last_key = pq.ParquetFile(directory / TRADES), {}, (-1, -1)
    for g in range(source.num_row_groups):
        table = source.read_row_group(g)
        t, t_code = _decode_trades(table, midnight)
        rows = Columns(None, None, None, master, batches, t, t_code)
        rows.trade_ids(carry)
        require(plain(table).equals(plain(rows._trades(midnight, slice(None)))), f'Non-canonical {TRADES}')
        identity(t['issue_id'], t_code)
        keys = t['issue_id'] << 32 | t['seq']
        require(not len(keys) or ((np.diff(keys) >= 0).all() and keys[0] >= (last_key[0] << 32 | last_key[1])),
                'Trades not sorted by issue, then batch')
        if len(keys):
            last_key = (int(t['issue_id'][-1]), int(t['seq'][-1]))
        batch_of(t['seq'], ['bootstrap', 'quotes', 'history'], t['time_us'], 'Trades')
        require(all(((v >= 0) & (v < 2**53)).all() for v in t.values()), 'Invalid unsigned integer')
        require((t['price10'] > 0).all() and (t['quantity'] > 0).all() and (t['frame'] < 2**32).all(), 'Invalid trade')
        trades += _per_batch(t['seq'], n)
        trade_bytes += int(trade_lengths(t, t_code).sum())
    require(quotes[0] + len(batches[0].get(SKIPPED, ())) == len(master), 'Incomplete bootstrap')
    history = kinds == 'history'
    require(((trades[history] > 0) & (trades[history] <= MAX_HISTORY)).all(), 'Invalid trades')
    require(batches[-1]['quote_updates'] == _updates(batches, quotes), 'End summary mismatch')
    summary = dict(source=batches[0]['source'], trading_date=batches[0]['trading_date'],
                   first_source_time_us=batches[0]['source_time_us'], last_source_time_us=batches[-1]['source_time_us'],
                   codes=sorted(m['code'] for m in master), batches=n, quote_updates=int(quotes.sum()),
                   expanded_bytes=batch_lengths(batches, master, quotes, trades) + quote_bytes + trade_bytes)
    require(manifest is None or summary == manifest['summary'], 'Summary mismatch')
    return summary


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def inspect_package(directory, manifest, references=None):
    """Verify a package directory against its manifest: its zip's size and hash, then its content."""
    validate_manifest(manifest)
    with tempfile.TemporaryDirectory() as tmp:
        sha256, size = pack_to(directory, Path(tmp) / 'recording.zip')
    require(size == manifest['bytes'] and sha256 == manifest['sha256'], 'Size/hash mismatch')
    return verify_package(directory, manifest, references)


def verify_directory(directory, manifest, references=None):
    """Verify an upload's tables (all four, in `directory`) against its manifest's summary: their content
    validates, and every value is its canonical encoding (see check and verify_tables). Nothing is written.
    Checked whole, in memory: a demo or synthetic recording, within their limits whatever its tables say."""
    validate_manifest(manifest)
    check(directory, broker=False)
    columns = Columns.load(directory)
    summary = validate_batches(columns.iter_batches(), references)
    require(summary == manifest['summary'], 'Summary mismatch')
    verify_tables(directory, columns)
    return summary


# NautilusTrader v2 catalog

def _stamp(ns: int) -> str:
    moment = dt.datetime.fromtimestamp(ns // 10**9, dt.timezone.utc)
    return moment.strftime('%Y-%m-%dT%H-%M-%S-') + f'{ns % 10**9:09d}Z'


def export_nautilus(directory, output) -> list[Path]:
    """Write a NautilusTrader v2 catalog: Equity, OrderBookDepth and TradeTick per instrument, and BriskQuote
    as custom data.

    BriskQuote holds the rest of each quote in vendor units (prices in tenths of a yen, times in
    microseconds after JST midnight, zero for none), as int64 columns a v2 `customdataclass` can declare,
    and the closing-condition books as JSON text ([[price10, quantity, orders], ...], best first).
    """
    directory, output = Path(directory), Path(output)
    check(directory)
    equity = pq.read_table(directory / EQUITY)
    batches = _decode_batches(pq.read_table(directory / BATCHES))
    midnight, master = midnight_ns(batches[0]['trading_date']), _decode_equity(equity)
    written = []

    def put(path, table):
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path, compression='zstd')
        written.append(path)

    info = pa.json_(pa.string())
    for i, ident in enumerate(equity.column('id').to_pylist()):
        row = equity.slice(i, 1).select(V2_EQUITY)
        row = row.set_column(row.schema.get_field_index('info'), pa.field('info', info), row.column('info').cast(info))
        ts = row.column('ts_init')[0].cast(pa.int64()).as_py()
        put(output / 'data' / 'equity' / ident / f'{_stamp(ts)}_{_stamp(ts)}.parquet',
            row.replace_schema_metadata({'instrument_id': ident, 'type_name': 'Equity'}))
    def instrument(ident, tables, pieces):
        rows, quoted = pa.concat_tables(tables).combine_chunks(), _join(pieces)
        n = len(quoted)
        ts = rows.column('ts_init').cast(pa.int64())
        span = f'{_stamp(ts[0].as_py())}_{_stamp(ts[-1].as_py())}.parquet'
        put(output / 'data' / 'order_book_depths' / ident / span, rows.select(V2_DEPTH).replace_schema_metadata(
            {'instrument_id': ident, 'price_precision': '1', 'size_precision': '0', 'type_name': 'OrderBookDepth'}))
        fields = {'instrument_id': pa.array([ident] * n)}
        fields |= {k: pa.array(quoted.q[k], pa.int64()) for k in INTS}
        quotes = quoted.quotes(np.arange(n))
        fields |= {name: pa.array([json.dumps(q[side], separators=(',', ':')) for q in quotes])
                   for side, name in BOOKS.items() if side.endswith('closing_levels')}
        fields |= {'type': pa.array(['BriskQuote'] * n), 'ts_event': rows.column('ts_event'), 'ts_init': rows.column('ts_init'),
                   'data_type': pa.array([json.dumps({'type_name': 'BriskQuote', 'metadata': None, 'identifier': ident},
                                                     separators=(',', ':'))] * n)}
        put(output / 'data' / 'custom' / 'BriskQuote' / ident / span,
            pa.table(fields).replace_schema_metadata({'type_name': 'BriskQuote'}))

    # An instrument at a time (rows are sorted by issue), a row group read at a time: never the whole in memory.
    source, current, tables, pieces = pq.ParquetFile(directory / DEPTHS), None, [], []
    for g in range(source.num_row_groups):
        table = source.read_row_group(g)
        decoded = _depth_rows(table, midnight, master)
        ids = table.column('identifier').to_pylist()
        starts = [i for i in range(len(ids)) if i == 0 or ids[i] != ids[i - 1]]
        for start, end in zip(starts, starts[1:] + [len(ids)]):
            if ids[start] != current and tables:
                instrument(current, tables, pieces)
                tables, pieces = [], []
            current = ids[start]
            tables.append(table.slice(start, end - start))
            pieces.append(_rows(decoded, np.arange(start, end)))
    if tables:
        instrument(current, tables, pieces)
    trades = pq.read_table(directory / TRADES)
    trades = trades.set_column(2, 'aggressor_side', trades.column(2).cast(pa.string()).dictionary_encode().cast(
        pa.dictionary(pa.int8(), pa.string())))
    ids = trades.column('identifier').to_pylist()
    starts = [i for i in range(len(ids)) if i == 0 or ids[i] != ids[i - 1]]
    for start, end in zip(starts, starts[1:] + [len(ids)]):
        ident, rows = ids[start], trades.slice(start, end - start)
        ts = rows.column('ts_init').cast(pa.int64())
        put(output / 'data' / 'trades' / ident / f'{_stamp(ts[0].as_py())}_{_stamp(ts[-1].as_py())}.parquet',
            rows.select(V2_TRADE).replace_schema_metadata(
                {'instrument_id': ident, 'price_precision': '1', 'size_precision': '0', 'type_name': 'TradeTick'}))
    return written
