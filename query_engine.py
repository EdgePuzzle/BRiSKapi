"""The cloud API's engine: Databento-style records from stored recordings, and their OHLCV bars.

Server side only (the query and archive functions); not part of the briskapi package, so bars come
from the cloud. Everything reads the archive bucket through `Bucket`, a boto3-style client.

Datasets follow Databento's VENUE.FEED naming: XTKS.BRISK is an owner's live recordings (any broker's
BRiSK: the same app and stream), XTKS.BRISKDEMO the public demo's market tables, kept once in the
archive. OHLCV bars for XTKS.BRISK are an owner's own: built ahead from its current live recordings of
each trading day (deduplicated by trade) whenever one of them is stored, deleted or restored, holding only
the bars whose time span within the trading sessions those recordings cover. No one else's uploads enter
them, and an owner's own are complete there: a stored recording accounts for every trade between its first
and last quote of each security, or its cumulative volumes would refuse it.
"""
from __future__ import annotations

import datetime as dt
import io
import json
import re
from typing import NamedTuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from briskapi._parquet import BATCHES, DEPTHS, EQUITY, FILES, MARKET, TRADES, midnight_ns
from briskapi.schema import RECORDING_PREFIX, REFERENCES, require

JST_NS = 9 * 3600 * 10**9
DAY_NS = 86400 * 10**9
LIVE = ('sbi_live', 'eshiten_live')
DEMO_SOURCE = 'historical_mock'
DATASETS = {'XTKS.BRISK': (1, 'Tokyo Stock Exchange via BRiSK'),
            'XTKS.BRISKDEMO': (2, 'Tokyo Stock Exchange, BRiSK Next demo replay')}
BARS = {'ohlcv-1s': 10**9, 'ohlcv-1m': 60 * 10**9, 'ohlcv-1h': 3600 * 10**9, 'ohlcv-1d': DAY_NS}
SCHEMAS = ('trades', 'mbp-10', 'definition', 'imbalance', 'brisk-quotes', *BARS)
ENCODINGS = {'parquet': ('application/vnd.apache.parquet', 'parquet'), 'csv': ('text/csv', 'csv'),
             'json': ('application/x-ndjson', 'json')}
MAX_DAYS, MAX_SYMBOLS = 31, 2000
# The times a request may name, as UTC nanoseconds: 2000 to 2100.
EARLIEST, LATEST = 946_684_800 * 10**9, 4_102_444_800 * 10**9
# The Tokyo Stock Exchange's trading sessions since 2024-11-05, as [start, end) seconds after JST midnight;
# the end second holds the closing auction's prints.
SESSIONS = ((9 * 3600, 11 * 3600 + 30 * 60 + 1), (12 * 3600 + 30 * 60, 15 * 3600 + 30 * 60 + 1))
BARS_ROOT = 'bars'    # bars/OWNER/YYYYMMDD/ohlcv-*.parquet
LEVELS = 10
TIME = pa.timestamp('ns', tz='UTC')


class NotFound(Exception):
    pass


# The bucket

class Bucket:
    """The archive bucket through a boto3-style client: listing, reads (ranged for Parquet), writes."""

    BLOCK = 4 * 1024 * 1024

    def __init__(self, s3, name):
        self.s3, self.name = s3, name

    def keys(self, prefix):
        return [item['Key'] for page in self.s3.get_paginator('list_objects_v2').paginate(Bucket=self.name, Prefix=prefix)
                for item in page.get('Contents', [])]

    def read(self, key, start=None, end=None):
        args = {'Range': f'bytes={start}-{end - 1}'} if start is not None else {}
        try:
            obj = self.s3.get_object(Bucket=self.name, Key=key, **args)
        except Exception as error:  # noqa: BLE001 - only a missing key is NotFound
            if getattr(error, 'response', {}).get('Error', {}).get('Code') in {'NoSuchKey', '404'}:
                raise NotFound(key) from None
            raise
        with obj['Body'] as body:
            return body.read()

    def json(self, key):
        return json.loads(self.read(key))

    def exists(self, key):
        try:
            self.s3.head_object(Bucket=self.name, Key=key)
            return True
        except Exception as error:  # noqa: BLE001
            if getattr(error, 'response', {}).get('Error', {}).get('Code') in {'NoSuchKey', '404', 'NotFound'}:
                return False
            raise

    def parquet(self, key):
        """A Parquet file read in 4 MiB blocks on demand: a query reads only the row groups it needs."""
        try:
            size = self.s3.head_object(Bucket=self.name, Key=key)['ContentLength']
        except Exception as error:  # noqa: BLE001 - only a missing key is NotFound
            if getattr(error, 'response', {}).get('Error', {}).get('Code') in {'NoSuchKey', '404', 'NotFound'}:
                raise NotFound(key) from None
            raise
        return pq.ParquetFile(_Ranged(self, key, size))

    def write(self, key, data, content_type='application/octet-stream'):
        self.s3.put_object(Bucket=self.name, Key=key, Body=data, ContentType=content_type)

    def delete(self, key):
        self.s3.delete_object(Bucket=self.name, Key=key)


class _Ranged(io.RawIOBase):
    def __init__(self, bucket, key, size):
        self.bucket, self.key, self.size, self.at, self.blocks = bucket, key, size, 0, {}

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.at

    def seek(self, offset, whence=0):
        self.at = {0: offset, 1: self.at + offset, 2: self.size + offset}[whence]
        return self.at

    def readinto(self, buffer):
        n = min(len(buffer), self.size - self.at)
        out, done = memoryview(buffer), 0
        while done < n:
            block, offset = divmod(self.at + done, Bucket.BLOCK)
            if block not in self.blocks:
                start = block * Bucket.BLOCK
                self.blocks[block] = self.bucket.read(self.key, start, min(start + Bucket.BLOCK, self.size))
            data = self.blocks[block][offset:offset + n - done]
            out[done:done + len(data)] = data
            done += len(data)
        self.at += n
        return n


# Recordings

class Recording(NamedTuple):
    prefix: str
    date: str          # YYYYMMDD, the trading date
    first: int         # UTC ns of the first and last source clock
    last: int
    codes: frozenset
    files: dict        # table name -> key
    receipt: bool      # whether the batches table (local receipt times) exists


def recordings(bucket, owner, dates=None):
    """An owner's live recordings (XTKS.BRISK), for the given trading dates (all when None)."""
    keys = bucket.keys(f'archive/{owner}/') if dates is None else \
        [key for date in dates for key in bucket.keys(f'archive/{owner}/{date}/')]
    found = []
    for key in sorted(keys):
        prefix = key.rsplit('/', 1)[0]
        if not key.endswith('/manifest.json') or not RECORDING_PREFIX.fullmatch(prefix):
            continue
        summary = bucket.json(key)['summary']
        if summary['source'] in LIVE:
            found.append(_recording(prefix, summary, bucket.json(f'{prefix}/files.json'), receipt=True))
    return found


def _recording(prefix, summary, files, receipt):
    midnight = midnight_ns(summary['trading_date'])
    return Recording(prefix, summary['trading_date'], midnight + summary['first_source_time_us'] * 1000,
                     midnight + summary['last_source_time_us'] * 1000, frozenset(summary['codes']), files, receipt)


def demo(bucket):
    """The public demo's market tables, as the archive keeps them once."""
    reference = REFERENCES[DEMO_SOURCE]
    prefix = f"reference/{DEMO_SOURCE}/{reference['timeline']}"
    if not bucket.exists(f'{prefix}/complete.json'):
        raise NotFound('The archive holds no copy of the demo yet')
    equity = pq.read_table(io.BytesIO(bucket.read(f'{prefix}/{EQUITY}')))
    meta = bucket.parquet(f'{prefix}/{DEPTHS}').metadata
    column = _leaf(meta, 'ts_event')
    stats = [meta.row_group(g).column(column).statistics for g in range(meta.num_row_groups)]
    midnight = midnight_ns(reference['trading_date'])
    first, last = min(_ns(s.min) for s in stats), max(_ns(s.max) for s in stats)
    summary = {'trading_date': reference['trading_date'], 'first_source_time_us': (first - midnight) // 1000,
               'last_source_time_us': (last - midnight) // 1000, 'codes': equity.column('raw_symbol').to_pylist()}
    return _recording(prefix, summary, {name: f'{prefix}/{name}' for name in MARKET}, receipt=False)


# Requests

def timestamp(value, default=None) -> int:
    """UTC nanoseconds from an integer (ns), a date (the JST trading day's midnight) or an ISO 8601 time
    (JST unless it says otherwise)."""
    if value is None or value == '':
        require(default is not None, 'start is required')
        return default
    if isinstance(value, int) or re.fullmatch(r'\d{10,}', str(value)):
        return int(value)
    text = str(value).strip().replace('Z', '+00:00')
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', text):
        return midnight_ns(text.replace('-', ''))
    try:
        moment = dt.datetime.fromisoformat(text)
    except ValueError:
        raise ValueError(f'Invalid time {value!r}: use ISO 8601, a date or UTC nanoseconds') from None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=dt.timezone(dt.timedelta(hours=9)))
    try:
        return int(moment.timestamp()) * 10**9 + moment.microsecond * 1000
    except OverflowError:
        raise ValueError(f'Invalid time {value!r}: out of range') from None


def trading_dates(start, end):
    """The JST trading dates [start, end) touches."""
    first, last = (start + JST_NS) // DAY_NS, (end - 1 + JST_NS) // DAY_NS
    require(last - first < MAX_DAYS, f'At most {MAX_DAYS} days per request')
    return [(dt.date(1970, 1, 1) + dt.timedelta(days=day)).strftime('%Y%m%d') for day in range(first, last + 1)]


def symbols_of(symbols):
    if symbols in (None, '', 'ALL_SYMBOLS') or symbols == ['ALL_SYMBOLS']:
        return None
    codes = [str(s).strip() for s in (symbols.split(',') if isinstance(symbols, str) else symbols) if str(s).strip()]
    require(codes and len(codes) <= MAX_SYMBOLS, f'Give 1 to {MAX_SYMBOLS} symbols, or ALL_SYMBOLS')
    for code in codes:
        require(re.fullmatch(r'[0-9A-Z]{4,5}', code), f'Invalid symbol {code!r}')
    return set(codes)


def instrument_id(code: str) -> int:
    """A stable uint32 per JPX code: digits-only codes are their number, others 100,000 plus their base-36 value."""
    return int(code) if code.isdigit() else 100_000 + int(code, 36)


class Request(NamedTuple):
    dataset: str
    schema: str
    symbols: set | None
    start: int
    end: int
    limit: int | None


def request(dataset, schema='trades', symbols=None, start=None, end=None, limit=None, stype_in='raw_symbol', **_) -> Request:
    require(dataset in DATASETS, f'Unknown dataset {dataset!r}; one of {", ".join(DATASETS)}')
    require(schema in SCHEMAS, f'Unknown schema {schema!r}; one of {", ".join(SCHEMAS)}')
    require(stype_in in (None, '', 'raw_symbol'), 'Only stype_in=raw_symbol (JPX codes) is supported')
    begin = timestamp(start)
    finish = timestamp(end, default=(begin + JST_NS) // DAY_NS * DAY_NS - JST_NS + DAY_NS)
    require(EARLIEST <= begin < finish <= LATEST, 'end must be after start, both between 2000 and 2100')
    trading_dates(begin, finish)
    count = None if limit in (None, '', 0, '0') else int(limit)
    require(count is None or count > 0, 'limit must be positive')
    return Request(dataset, schema, symbols_of(symbols), begin, finish, count)


# Reading a recording's tables

def _ids(bucket, rec, symbols):
    """{issue_id: code} for the requested codes in this recording's master."""
    equity = pq.read_table(io.BytesIO(bucket.read(rec.files[EQUITY])), columns=['raw_symbol', 'issue_id'])
    return {i: c for c, i in zip(equity.column('raw_symbol').to_pylist(), equity.column('issue_id').to_pylist())
            if symbols is None or c in symbols}


def _rows(bucket, rec, name, ids, start, end, columns=None):
    """A table's rows for these issues within [start, end): only row groups whose statistics can match."""
    source = bucket.parquet(rec.files[name])
    meta = source.metadata
    leaves = [_leaf(meta, 'issue_id'), _leaf(meta, 'ts_event')]
    low, high = (min(ids), max(ids)) if ids else (0, -1)
    groups = []
    for g in range(meta.num_row_groups):
        stats = {meta.row_group(g).column(c).path_in_schema: meta.row_group(g).column(c).statistics
                 for c in leaves if c is not None}
        issue = stats.get('issue_id')
        if issue is not None and issue.has_min_max and (issue.max < low or issue.min > high):
            continue
        ts = stats.get('ts_event')
        if ts is not None and ts.has_min_max and (_ns(ts.max) < start or _ns(ts.min) >= end):
            continue
        groups.append(g)
    if not groups or not ids:
        return source.schema_arrow.empty_table() if columns is None else \
            pa.schema([source.schema_arrow.field(c) for c in columns]).empty_table()
    table = source.read_row_groups(groups, columns=columns)
    ts = pc.cast(table.column('ts_event'), pa.int64())
    keep = pc.and_(pc.is_in(table.column('issue_id'), value_set=pa.array(sorted(ids), pa.uint64())),
                   pc.and_(pc.greater_equal(ts, start), pc.less(ts, end)))
    return table.filter(keep)


def _leaf(meta, name):
    """A top-level column's index among the file's leaf columns (nested book lists add leaves before it)."""
    if not meta.num_row_groups:
        return None
    group = meta.row_group(0)
    return next((c for c in range(group.num_columns) if group.column(c).path_in_schema == name), None)


def _ns(value):
    if isinstance(value, dt.datetime):
        return int(value.timestamp()) * 10**9 + value.microsecond * 1000
    return int(value)


def _receipts(bucket, rec, seq, ts_event):
    """Local receipt times (UTC ns) for rows of these batches; the source clock for a replay copy."""
    if not rec.receipt or rec.files.get(BATCHES) is None:
        return ts_event
    batches = pq.read_table(io.BytesIO(bucket.read(rec.files[BATCHES])), columns=['seq', 'received_unix_ms'])
    received = np.zeros(int(pc.max(batches.column('seq')).as_py() or 0) + 1, np.int64)
    received[batches.column('seq').to_numpy()] = batches.column('received_unix_ms').fill_null(0).to_numpy().astype(np.int64) * 10**6
    values = received[seq.to_numpy()]
    return pa.array(np.where(values > 0, values, pc.cast(ts_event, pa.int64()).to_numpy()), TIME)


def _codes(table):
    return pc.replace_substring_regex(table.column('identifier'), r'\.XTKS$', '')


def _common(table, rec, publisher, bucket):
    codes = _codes(table)
    ids = pa.array([instrument_id(c) for c in codes.to_pylist()], pa.uint32())
    ts_event = table.column('ts_event').combine_chunks() if table.num_rows else pa.array([], TIME)
    seq = table.column('seq').combine_chunks() if table.num_rows else pa.array([], pa.uint64())
    ts_recv = _receipts(bucket, rec, seq, ts_event) if table.num_rows else pa.array([], TIME)
    return {'ts_recv': pc.cast(ts_recv, TIME), 'ts_event': pc.cast(ts_event, TIME),
            'publisher_id': pa.array(np.full(table.num_rows, publisher, np.uint16)), 'instrument_id': ids}, codes


def _price(column):
    return pc.cast(column, pa.float64())


def _flat_levels(books, name):
    """bid/ask levels 00-09 (price, size, count) from a list<struct> column, null past a side's last level."""
    books = books.combine_chunks() if isinstance(books, pa.ChunkedArray) else books
    lengths = pc.list_value_length(books).fill_null(0).to_numpy()
    offsets = books.offsets.to_numpy()[:-1]
    values = books.values
    out = {}
    for level in range(LEVELS):
        valid = lengths > level
        index = pa.array(np.where(valid, offsets + level, 0), pa.int64())
        mask = pa.array(~valid)
        for field, key, kind in (('price', 'px', pa.float64()), ('size', 'sz', pa.uint64()), ('count', 'ct', pa.uint32())):
            column = values.field(field) if len(values) else pa.array([], values.type.field(field).type)
            taken = column.take(index) if len(values) else pa.nulls(len(index), column.type)
            taken = pc.cast(taken, kind)
            out[f'{name}_{key}_{level:02d}'] = pc.if_else(mask, pa.scalar(None, kind), taken)
    return out


def _project(bucket, rec, req, publisher):
    """One recording's records of the requested (non-bar) schema."""
    ids = _ids(bucket, rec, req.symbols)
    if req.schema == 'definition':
        return _definition(bucket, rec, ids, req, publisher)
    name = TRADES if req.schema == 'trades' else DEPTHS
    table = _rows(bucket, rec, name, ids, req.start, req.end)
    common, codes = _common(table, rec, publisher, bucket)
    n = table.num_rows
    if req.schema == 'trades':
        side = pc.cast(table.column('aggressor_side'), pa.string()) if n else pa.array([], pa.string())
        return pa.table({**common, 'action': pa.array(['T'] * n, pa.string()),
                         'side': pc.if_else(pc.equal(side, 'BUY'), 'B', pc.if_else(pc.equal(side, 'SELL'), 'A', 'N'))
                         if n else pa.array([], pa.string()),
                         'depth': pa.array(np.zeros(n, np.uint8)), 'price': _price(table.column('price')),
                         'size': pc.cast(table.column('size'), pa.uint64()), 'flags': pa.array(np.zeros(n, np.uint8)),
                         'sequence': table.column('frame'), 'trade_id': table.column('trade_id'), 'symbol': codes})
    if req.schema == 'mbp-10':
        # BRiSK gives each update's book, not the order event that changed it: action, side, depth, price
        # and size are left empty ('N'), and the session's first rows carry Databento's snapshot flag (32).
        flags = pc.if_else(pc.equal(table.column('seq'), 0), pa.scalar(32, pa.uint8()), pa.scalar(0, pa.uint8())) \
            if n else pa.array([], pa.uint8())
        return pa.table({**common, 'action': pa.array(['N'] * n, pa.string()), 'side': pa.array(['N'] * n, pa.string()),
                         'depth': pa.array(np.zeros(n, np.uint8)), 'price': pa.nulls(n, pa.float64()),
                         'size': pa.array(np.zeros(n, np.uint64)), 'flags': flags, 'sequence': table.column('sequence'),
                         **_flat_levels(table.column('bids'), 'bid'), **_flat_levels(table.column('asks'), 'ask'),
                         'symbol': codes})
    if req.schema == 'imbalance':
        return _imbalance(bucket, rec, table, common, codes, ids)
    # brisk-quotes: everything BRiSK records for each update.
    drop = {'identifier', 'ts_event', 'ts_init', 'issue_id', 'seq'}
    rest = {}
    for name in table.column_names:
        if name in drop:
            continue
        kind = table.schema.field(name).type
        rest[name] = _book(table.column(name)) if pa.types.is_list(kind) else \
            _price(table.column(name)) if pa.types.is_decimal(kind) else table.column(name)
    return pa.table({**common, **rest, 'symbol': codes})


BOOK = pa.list_(pa.field('item', pa.struct([('price', pa.float64()), ('size', pa.uint64()), ('count', pa.uint32())]), False))


def _book(column):
    """Book levels as {price (yen), size (shares), count (orders)}, best first."""
    books = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    if not len(books):
        return pa.array([], BOOK)
    values = books.values
    levels = pa.StructArray.from_arrays([_price(values.field('price')), pc.cast(values.field('size'), pa.uint64()),
                                         values.field('count')], names=['price', 'size', 'count'])
    return pa.ListArray.from_arrays(books.offsets, levels, type=BOOK)


def _definition(bucket, rec, ids, req, publisher):
    equity = pq.read_table(io.BytesIO(bucket.read(rec.files[EQUITY])))
    equity = equity.filter(pc.is_in(equity.column('issue_id'), value_set=pa.array(sorted(ids), pa.uint64())))
    midnight = midnight_ns(rec.date)
    if not (req.start <= midnight + DAY_NS - 1 and midnight < req.end):
        equity = equity.slice(0, 0)
    n = equity.num_rows
    codes = equity.column('raw_symbol')
    number = lambda name: pc.cast(equity.column(name), pa.float64())  # noqa: E731
    return pa.table({'ts_recv': pa.array(np.full(n, midnight), TIME), 'ts_event': pa.array(np.full(n, midnight), TIME),
                     'publisher_id': pa.array(np.full(n, publisher, np.uint16)),
                     'instrument_id': pa.array([instrument_id(c) for c in codes.to_pylist()], pa.uint32()),
                     'raw_symbol': codes, 'exchange': pa.array(['XTKS'] * n, pa.string()),
                     'currency': pa.array(['JPY'] * n, pa.string()),
                     'min_lot_size_round_lot': pc.cast(pc.cast(equity.column('lot_size'), pa.float64()), pa.int64()),
                     'min_price_increment': number('price_increment'), 'high_limit_price': number('max_price'),
                     'low_limit_price': number('min_price'), 'base_price': _price(equity.column('base_price')),
                     'name': equity.column('name'), 'issue_type': equity.column('issue_type'),
                     'tick_type': equity.column('tick_type'), 'symbol': codes})


def _imbalance(bucket, rec, table, common, codes, ids):
    """Each update's opening (O) and closing (C) auction state, where BRiSK shows an indicative price."""
    limits = pq.read_table(io.BytesIO(bucket.read(rec.files[EQUITY])), columns=['issue_id', 'max_price', 'min_price'])
    upper = dict(zip(limits.column('issue_id').to_pylist(), pc.cast(limits.column('max_price'), pa.float64()).to_pylist()))
    lower = dict(zip(limits.column('issue_id').to_pylist(), pc.cast(limits.column('min_price'), pa.float64()).to_pylist()))
    issue = table.column('issue_id').to_pylist()
    parts = []
    for auction, prefix, book in (('O', '', ''), ('C', 'closing_', '_closing')):
        price = table.column(f'{prefix}indicative_price')
        keep = pc.is_valid(price)
        buy = pc.cast(table.column(f'{prefix}market_buy_quantity'), pa.int64())
        sell = pc.cast(table.column(f'{prefix}market_sell_quantity'), pa.int64())
        imbalance = pc.subtract(buy, sell)
        n = table.num_rows
        part = pa.table({**common, 'auction_type': pa.array([auction] * n, pa.string()),
                         'ref_price': _price(table.column('reference_price')), 'ind_match_price': _price(price),
                         'paired_qty': table.column(f'{prefix}indicative_volume'),
                         'side': pc.if_else(pc.equal(table.column(f'{prefix}indicative_imbalance_side'), 1), 'A', 'B')
                         if n else pa.array([], pa.string()),
                         'market_buy_qty': table.column(f'{prefix}market_buy_quantity'),
                         'market_sell_qty': table.column(f'{prefix}market_sell_quantity'),
                         'market_imbalance_qty': pc.cast(pc.abs(imbalance), pa.uint64()),
                         'market_imbalance_side': pc.if_else(pc.greater(imbalance, 0), 'B',
                                                             pc.if_else(pc.less(imbalance, 0), 'A', 'N')) if n else pa.array([], pa.string()),
                         'bid_total_qty': table.column(f'bid{book}_total_quantity'),
                         'ask_total_qty': table.column(f'ask{book}_total_quantity'),
                         'upper_collar': pa.array([upper.get(i) for i in issue], pa.float64()),
                         'lower_collar': pa.array([lower.get(i) for i in issue], pa.float64()),
                         'auction_time': table.column('special_quote_time'), 'sequence': table.column('sequence'),
                         'symbol': codes})
        parts.append(part.filter(keep))
    return pa.concat_tables(parts)


# Bars

def build_bars(trades: list[pa.Table]) -> dict[str, pa.Table]:
    """OHLCV bars per security from trades (code, trade_id, frame, ts_event, price, size), deduplicated by
    (code, trade_id) across captures; intervals aligned to JST."""
    if not trades:
        return {schema: _empty_bars() for schema in BARS}
    table = pa.concat_tables([t.select(['symbol', 'trade_id', 'sequence', 'ts_event', 'price', 'size']) for t in trades])
    if len(trades) > 1:
        table = _dedupe(table, ['symbol', 'trade_id'])
    place = pc.cast(pc.list_element(pc.split_pattern(table.column('trade_id'), '-'), 1), pa.int64())
    table = table.append_column('place', place).sort_by([('symbol', 'ascending'), ('ts_event', 'ascending'),
                                                          ('sequence', 'ascending'), ('place', 'ascending')])
    ts = pc.cast(table.column('ts_event'), pa.int64()).to_numpy()
    out = {}
    for schema, length in BARS.items():
        start = (ts + JST_NS) // length * length - JST_NS
        grouped = table.append_column('bar', pa.array(start, pa.int64())).group_by(['symbol', 'bar'], use_threads=False) \
            .aggregate([('price', 'first'), ('price', 'max'), ('price', 'min'), ('price', 'last'), ('size', 'sum')])
        bars = pa.table({'ts_event': pc.cast(grouped.column('bar'), TIME), 'symbol': grouped.column('symbol'),
                         'open': grouped.column('price_first'), 'high': grouped.column('price_max'),
                         'low': grouped.column('price_min'), 'close': grouped.column('price_last'),
                         'volume': pc.cast(grouped.column('size_sum'), pa.uint64())})
        out[schema] = bars.sort_by([('symbol', 'ascending'), ('ts_event', 'ascending')])
    return out


def _empty_bars():
    return pa.schema([('ts_event', TIME), ('symbol', pa.string()), ('open', pa.float64()), ('high', pa.float64()),
                      ('low', pa.float64()), ('close', pa.float64()), ('volume', pa.uint64())]).empty_table()


def _trades_for_bars(bucket, rec):
    table = pq.read_table(io.BytesIO(bucket.read(rec.files[TRADES])),
                          columns=['identifier', 'trade_id', 'frame', 'ts_event', 'price', 'size'])
    return pa.table({'symbol': _codes(table), 'trade_id': table.column('trade_id'), 'sequence': table.column('frame'),
                     'ts_event': table.column('ts_event'), 'price': _price(table.column('price')),
                     'size': pc.cast(table.column('size'), pa.uint64())})


def bars_root(owner, date):
    return f'{BARS_ROOT}/{owner}/{date}'


def rebuild_bars(bucket, owner, date, attempts=3):
    """Build an owner's bars for a trading day from its current live recordings of that day, covered as `covered`
    says; with none left, remove them. Rebuilds of one owner's day are not serialized, so one that finds its
    recordings changed meanwhile (another upload, delete or restore) runs again."""
    for _ in range(attempts):
        before = bucket.keys(f'archive/{owner}/{date}/')
        built = _rebuild(bucket, owner, date, recordings(bucket, owner, [date]))
        if bucket.keys(f'archive/{owner}/{date}/') == before:
            return built
    return built


def _rebuild(bucket, owner, date, recs):
    root = bars_root(owner, date)
    if not recs:
        for key in bucket.keys(f'{root}/'):
            bucket.delete(key)
        return 0
    for schema, bars in build_bars([_trades_for_bars(bucket, r) for r in recs]).items():
        out = io.BytesIO()
        # Sorted by symbol in 65,536-row groups: a query for a few securities reads only theirs.
        pq.write_table(covered(bars, BARS[schema], recs), out, compression='zstd', row_group_size=65_536)
        bucket.write(f'{root}/{schema}.parquet', out.getvalue(), 'application/vnd.apache.parquet')
    return len(recs)


def covered(bars, length, recs):
    """The bars an owner's recordings cover: those whose span, clipped to the trading sessions, lies within the
    owner's coverage of that security (the union of its recordings' clock ranges). A bar its recordings cover only
    in part would be missing trades."""
    if not bars.num_rows:
        return bars
    starts = pc.cast(bars.column('ts_event'), pa.int64()).to_numpy()
    pieces = _session_pieces(starts, starts + length)
    symbols = bars.column('symbol')
    # Securities in the same recordings share their windows (usually every security is in every recording), so
    # each such group is one vectorized pass, not one per security.
    groups = {}
    for code in pc.unique(symbols).to_pylist():
        groups.setdefault(tuple(i for i, r in enumerate(recs) if code in r.codes), []).append(code)
    keep = np.zeros(bars.num_rows, bool)
    for members, codes in groups.items():
        windows = _merge([(recs[i].first, recs[i].last + 1) for i in members])
        if not windows:
            continue
        lows, highs = np.array([w[0] for w in windows]), np.array([w[1] for w in windows])
        rows = np.nonzero(pc.is_in(symbols, value_set=pa.array(codes)).to_numpy())[0]
        ok = np.ones(len(rows), bool)
        for low, high in pieces:
            low, high = low[rows], high[rows]
            where = np.searchsorted(lows, low, 'right') - 1
            ok &= (high <= low) | ((where >= 0) & (highs[np.maximum(where, 0)] >= high))
        keep[rows] = ok
    return bars.filter(pa.array(keep))


def _merge(windows):
    merged = []
    for low, high in sorted(windows):
        if merged and low <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], high)
        else:
            merged.append([low, high])
    return merged


def _session_pieces(begin, end):
    """[begin, end) clipped to each trading session of its JST day; a bar outside the sessions keeps its span."""
    day = (begin + JST_NS) // DAY_NS * DAY_NS - JST_NS
    pieces = [(np.maximum(begin, day + low * 10**9), np.minimum(end, day + high * 10**9)) for low, high in SESSIONS]
    # A bar longer than a day (none) or outside every session is judged on its whole span.
    outside = np.all([high <= low for low, high in pieces], axis=0)
    return [(np.where(outside, begin, low), np.where(outside, end, high)) for low, high in pieces]


def _dedupe(table, keys):
    joined = pc.binary_join_element_wise(*[pc.cast(table.column(k), pa.string()) for k in keys], '\x1f')
    first = pa.table({'k': joined, 'i': pa.array(np.arange(table.num_rows))}).group_by('k', use_threads=False) \
        .aggregate([('i', 'min')]).column('i_min')
    return table.take(pa.array(np.sort(first.to_numpy())))


# Queries

def get_range(bucket, owner, req: Request) -> pa.Table:
    """The records of a request, merged across recordings, deduplicated, time-ordered, cut to `limit`."""
    publisher = DATASETS[req.dataset][0]
    dates = trading_dates(req.start, req.end)
    if req.dataset == 'XTKS.BRISKDEMO':
        rec = demo(bucket)
        recs = [rec] if rec.date in dates else []
    else:
        recs = recordings(bucket, owner, dates)
    if req.schema in BARS:
        table = _bars(bucket, owner, req, recs, publisher)
    else:
        parts = [_project(bucket, rec, req, publisher) for rec in recs]
        table = pa.concat_tables(parts) if parts else _empty(req.schema)
        if len(parts) > 1:
            key = {'trades': ['instrument_id', 'trade_id'], 'definition': ['instrument_id', 'ts_event'],
                   'imbalance': ['instrument_id', 'sequence', 'auction_type']}.get(req.schema, ['instrument_id', 'sequence'])
            table = _dedupe(table, key)
        order = [('ts_event', 'ascending'), ('instrument_id', 'ascending')] + \
            ([('sequence', 'ascending')] if 'sequence' in table.column_names else [])
        table = table.sort_by(order)
    return table.slice(0, req.limit) if req.limit else table


def _bars(bucket, owner, req, recs, publisher):
    length = BARS[req.schema]
    parts = []
    for date in sorted({r.date for r in recs}):
        day = [r for r in recs if r.date == date]
        if req.dataset == 'XTKS.BRISKDEMO':
            bars = build_bars([_trades_for_bars(bucket, r) for r in day])[req.schema]
        else:
            try:
                bars = _built(bucket, f'{bars_root(owner, date)}/{req.schema}.parquet', req.symbols)
            except NotFound:  # not built (a rebuild failed): from the recordings now
                bars = covered(build_bars([_trades_for_bars(bucket, r) for r in day])[req.schema], length, day)
        ts = pc.cast(bars.column('ts_event'), pa.int64())
        keep = pc.and_(pc.greater_equal(ts, req.start), pc.less(ts, req.end))
        if req.symbols is not None:
            keep = pc.and_(keep, pc.is_in(bars.column('symbol'), value_set=pa.array(sorted(req.symbols))))
        parts.append(bars.filter(keep))
    bars = pa.concat_tables(parts) if parts else _empty_bars()
    n = bars.num_rows
    table = pa.table({'ts_event': bars.column('ts_event'), 'publisher_id': pa.array(np.full(n, publisher, np.uint16)),
                      'instrument_id': pa.array([instrument_id(c) for c in bars.column('symbol').to_pylist()], pa.uint32()),
                      'open': bars.column('open'), 'high': bars.column('high'), 'low': bars.column('low'),
                      'close': bars.column('close'), 'volume': bars.column('volume'), 'symbol': bars.column('symbol')})
    return table.sort_by([('ts_event', 'ascending'), ('instrument_id', 'ascending')])


def _built(bucket, key, symbols):
    """A day's built bars, only the row groups whose symbol range can hold a requested one."""
    source = bucket.parquet(key)
    if symbols is None:
        return source.read()
    meta, at = source.metadata, _leaf(source.metadata, 'symbol')
    groups = []
    for g in range(meta.num_row_groups):
        stats = meta.row_group(g).column(at).statistics
        if stats is None or not stats.has_min_max or any(stats.min <= s <= stats.max for s in symbols):
            groups.append(g)
    return source.read_row_groups(groups) if groups else source.schema_arrow.empty_table()


def _empty(schema):
    return pa.schema([(name, kind) for name, kind, _ in FIELDS[schema]]).empty_table()


def encode(table, encoding='parquet', compression=None, pretty_ts=True, pretty_px=True, map_symbols=True):
    """(body, content type, file extension) of a result in one of our encodings."""
    require(encoding in ENCODINGS, f'Unknown encoding {encoding!r}; one of {", ".join(ENCODINGS)}')
    require(compression in (None, '', 'none', 'zstd'), 'compression is none or zstd')
    if not map_symbols and 'symbol' in table.column_names:
        table = table.drop_columns(['symbol'])
    content, extension = ENCODINGS[encoding]
    out = io.BytesIO()
    if encoding == 'parquet':
        pq.write_table(table, out, compression='zstd')
        return out.getvalue(), content, extension
    columns = {}
    for name in table.column_names:
        column, kind = table.column(name), table.schema.field(name).type
        if pa.types.is_timestamp(kind):
            column = pc.strftime(column, format='%Y-%m-%dT%H:%M:%SZ') if pretty_ts else pc.cast(column, pa.int64())
        elif pa.types.is_floating(kind) and not pretty_px and name not in {'replay_lateness_ms'}:
            column = pc.cast(pc.round(pc.multiply(column, 10**9)), pa.int64())
        elif pa.types.is_nested(kind):
            column = pa.array([None if v is None else json.dumps(v) for v in column.to_pylist()], pa.string()) \
                if encoding == 'csv' else column
        columns[name] = column
    table = pa.table(columns)
    if encoding == 'csv':
        pacsv.write_csv(table, out)
    else:
        for row in table.to_pylist():
            out.write((json.dumps(row, default=str) + '\n').encode())
    if compression == 'zstd':
        return pa.Codec('zstd').compress(out.getvalue(), asbytes=True), 'application/zstd', extension + '.zst'
    return out.getvalue(), content, extension


# Metadata

def fields(schema):
    """[{name, type, description}] of a schema's records, as returned (docs/data-format.md explains them)."""
    require(schema in SCHEMAS, f'Unknown schema {schema!r}')
    return [{'name': name, 'type': str(kind), 'description': text} for name, kind, text in FIELDS[schema]]


def dataset_dates(bucket, owner, dataset):
    """The trading dates with data, and each one's covered clock windows (JST HH:MM:SS)."""
    recs = [demo(bucket)] if dataset == 'XTKS.BRISKDEMO' else recordings(bucket, owner)
    days = {}
    for rec in recs:
        days.setdefault(rec.date, []).append((rec.first, rec.last))
    clock = lambda ns, date: (dt.datetime.fromtimestamp((ns - midnight_ns(date)) / 10**9, dt.timezone.utc)  # noqa: E731
                              .strftime('%H:%M:%S'))
    return {date: [[clock(low, date), clock(high, date)] for low, high in _merge(windows)] for date, windows in sorted(days.items())}


# Every schema's columns, in order: (name, Arrow type, meaning). get_range returns exactly these.
_HEAD = [('ts_recv', TIME, "When the uploader's computer received the frame (millisecond precision; the demo: ts_event)"),
         ('ts_event', TIME, "BRiSK's server stamp for the frame (microseconds); not established to be the exchange's time"),
         ('publisher_id', pa.uint16(), '1 for XTKS.BRISK, 2 for XTKS.BRISKDEMO (metadata.list_publishers)'),
         ('instrument_id', pa.uint32(), 'Fixed per code: digits-only codes are their number, others 100,000 plus base 36')]
_EVENT = [('action', pa.string(), None), ('side', pa.string(), None), ('depth', pa.uint8(), None),
          ('price', pa.float64(), None), ('size', pa.uint64(), None), ('flags', pa.uint8(), None),
          ('sequence', pa.uint64(), "BRiSK's frame number")]
_SYMBOL = [('symbol', pa.string(), 'The JPX code (left out with map_symbols=false)')]
_QUOTE_TEXT = {
    'bids': 'The ten best bid levels, best first: {price (yen), size (shares), count (orders)}',
    'asks': 'The ten best ask levels, best first: {price, size, count}',
    'flags': '16, plus 32 on a session\'s first update of each security (the snapshot)',
    'sequence': "BRiSK's frame number",
    'last_price': 'Last traded price (yen); null when BRiSK shows none',
    'open_price': "The session's opening price (yen)",
    'bid_price': 'Best bid (yen)', 'ask_price': 'Best ask (yen)',
    'indicative_price': 'Indicative matching price of the auction (yen), excluding closing-condition orders',
    'closing_indicative_price': "The closing-condition orders' indicative price (yen)",
    'indicative_open_price': 'Indicative opening price (yen)',
    'reference_price': 'Where indicative matching starts: the special quote price, else the last, else the base price (yen)',
    'max_frame': "The latest frame BRiSK had seen for this update",
    'indicative_volume': 'Shares that would match at the indicative price',
    'indicative_imbalance_side': '1 when sell exceeds buy at the indicative price (or nothing matches), else 0',
    'closing_indicative_volume': 'The same for the closing-condition orders',
    'closing_indicative_imbalance_side': 'The same for the closing-condition orders',
    'quote_flag': 'Special-quote flags (raw BRiSK value)', 'quote_side': 'Special-quote side (raw: 0 buy, 1 sell)',
    'volume': "The day's cumulative volume (shares)",
    'issue_status': "The broker's issue status (raw BRiSK value)",
    'market_buy_quantity': 'Market buy orders (shares)', 'market_sell_quantity': 'Market sell orders (shares)',
    'closing_market_buy_quantity': 'Closing-condition market buy orders (shares)',
    'closing_market_sell_quantity': 'Closing-condition market sell orders (shares)',
    'bid_quantity': 'Shares at the best bid', 'bid_orders': 'Orders at the best bid',
    'ask_quantity': 'Shares at the best ask', 'ask_orders': 'Orders at the best ask',
    'bid_total_quantity': 'Shares bid on the whole board', 'bid_total_orders': 'Bid orders on the whole board',
    'bid_closing_total_quantity': 'Closing-condition shares bid', 'bid_closing_total_orders': 'Closing-condition bid orders',
    'ask_total_quantity': 'Shares offered on the whole board', 'ask_total_orders': 'Ask orders on the whole board',
    'ask_closing_total_quantity': 'Closing-condition shares offered', 'ask_closing_total_orders': 'Closing-condition ask orders',
    'special_quote_time': "When the special quote's timer ends; null without one",
    'turnover': "The day's cumulative turnover (yen)",
    'closing_bids': "The closing-condition orders' bid levels, like bids", 'closing_asks': "The closing-condition orders' ask levels",
}


def _quote_fields():
    from briskapi._parquet import DEPTH_SCHEMA
    out = []
    for field in DEPTH_SCHEMA:
        if field.name in {'identifier', 'ts_event', 'ts_init', 'issue_id', 'seq'}:
            continue
        kind = BOOK if pa.types.is_list(field.type) else pa.float64() if pa.types.is_decimal(field.type) else field.type
        out.append((field.name, kind, _QUOTE_TEXT[field.name]))
    return out


_BAR = [('ts_event', TIME, "The bar's start (JST-aligned; a day bar starts at the trading date's JST midnight)"),
        *_HEAD[2:], ('open', pa.float64(), 'First trade price (yen)'), ('high', pa.float64(), 'Highest (yen)'),
        ('low', pa.float64(), 'Lowest (yen)'), ('close', pa.float64(), 'Last trade price (yen)'),
        ('volume', pa.uint64(), 'Shares traded'), *_SYMBOL]
FIELDS = {
    'trades': [*_HEAD, ('action', pa.string(), "'T'"),
               ('side', pa.string(), "The aggressor: 'B' buyer-initiated, 'A' seller-initiated, 'N' none (most opening prints)"),
               ('depth', pa.uint8(), '0'), ('price', pa.float64(), 'Trade price (yen)'), ('size', pa.uint64(), 'Shares'),
               ('flags', pa.uint8(), '0'), ('sequence', pa.uint64(), "BRiSK's frame number"),
               ('trade_id', pa.string(), "FRAME-PLACE: the frame and the trade's place in it; unique per instrument"),
               *_SYMBOL],
    'mbp-10': [*_HEAD, ('action', pa.string(), "'N': BRiSK sends books, not the order events that changed them"),
               ('side', pa.string(), "'N'"), ('depth', pa.uint8(), '0'), ('price', pa.float64(), 'null'),
               ('size', pa.uint64(), '0'), ('flags', pa.uint8(), "32 (snapshot) on a session's first update of each security, else 0"),
               ('sequence', pa.uint64(), "BRiSK's frame number"),
               *[(f'{side}_{key}_{level:02d}', kind, text.format(side=side, level=level))
                 for side in ('bid', 'ask') for level in range(LEVELS)
                 for key, kind, text in (('px', pa.float64(), '{side} level {level} price (yen; 00 is the best); null past the last level'),
                                         ('sz', pa.uint64(), '{side} level {level} shares'),
                                         ('ct', pa.uint32(), '{side} level {level} orders'))],
               *_SYMBOL],
    'definition': [('ts_recv', TIME, "The trading date's JST midnight"), ('ts_event', TIME, "The trading date's JST midnight"),
                   *_HEAD[2:], ('raw_symbol', pa.string(), 'The JPX code'), ('exchange', pa.string(), "'XTKS'"),
                   ('currency', pa.string(), "'JPY'"), ('min_lot_size_round_lot', pa.int64(), 'Shares per trading unit'),
                   ('min_price_increment', pa.float64(), "BRiSK's price resolution (0.1 yen), not the tick size, which depends on price and tick_type"),
                   ('high_limit_price', pa.float64(), "The day's upper price limit (yen)"),
                   ('low_limit_price', pa.float64(), "The day's lower price limit (yen)"),
                   ('base_price', pa.float64(), "The day's base price (yen)"), ('name', pa.string(), "The security's name"),
                   ('issue_type', pa.uint64(), 'Raw BRiSK issue type'), ('tick_type', pa.uint64(), 'Raw BRiSK tick table'),
                   *_SYMBOL],
    'imbalance': [*_HEAD, ('auction_type', pa.string(), "'O' the session's auction, 'C' the closing-condition orders'"),
                  ('ref_price', pa.float64(), 'Where indicative matching starts (yen; see reference_price)'),
                  ('ind_match_price', pa.float64(), 'Indicative matching price (yen)'),
                  ('paired_qty', pa.uint64(), 'Shares that would match there'),
                  ('side', pa.string(), "'A' when sell exceeds buy at that price (or nothing matches), else 'B'"),
                  ('market_buy_qty', pa.uint64(), 'Market buy orders (shares)'),
                  ('market_sell_qty', pa.uint64(), 'Market sell orders (shares)'),
                  ('market_imbalance_qty', pa.uint64(), '|market buy - market sell| (shares)'),
                  ('market_imbalance_side', pa.string(), "'B' more market buying, 'A' more selling, 'N' equal"),
                  ('bid_total_qty', pa.uint64(), 'Shares bid on the whole board'),
                  ('ask_total_qty', pa.uint64(), 'Shares offered on the whole board'),
                  ('upper_collar', pa.float64(), "The day's upper price limit (yen)"),
                  ('lower_collar', pa.float64(), "The day's lower price limit (yen)"),
                  ('auction_time', TIME, "When the special quote's timer ends; null without one"),
                  ('sequence', pa.uint64(), "BRiSK's frame number"), *_SYMBOL],
    'brisk-quotes': [*_HEAD, *_quote_fields(), *_SYMBOL],
    **{schema: _BAR for schema in BARS},
}
