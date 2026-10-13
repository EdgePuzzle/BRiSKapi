"""Bounded validation of decoded batches, shared by local packaging and automatic ingest.

The archive accepts recordings with exact market/timing fields, checked batch by batch
in their canonical JSON form whatever the file format (see briskapi/_parquet.py).
Demo and synthetic market content must match a pinned reference replay. Opted-in
broker recordings (SBI, e-shiten) have structural and continuity checks, but their market content
cannot be independently verified. The service writes accepted content itself.
"""
import datetime as dt
import hashlib
import io
import json
import math
from pathlib import Path
import re

MAX_COMPRESSED = 64 * 1024 * 1024
# Bounds on the canonical JSON form of a recording and of each batch.
MAX_EXPANDED = 1024 * 1024 * 1024
MAX_LINE = 16 * 1024 * 1024
SCHEMA = 'brisk-nautilus-parquet-v1'
SOURCES = {'historical_mock', 'synthetic_test', 'sbi_live', 'eshiten_live'}
QUOTE_KEYS = set('issue_id code frame source_time_us max_frame last_price10 open_price10 bid_price10 ask_price10 indicative_price10 indicative_volume indicative_imbalance_side closing_indicative_price10 closing_indicative_volume closing_indicative_imbalance_side quote_flag quote_side special_quote_time_us indicative_open_price10 reference_price10 volume turnover10 issue_status market_buy_quantity market_sell_quantity closing_market_buy_quantity closing_market_sell_quantity bid_quantity bid_orders ask_quantity ask_orders bid_levels ask_levels'.split())
# A side of the book: up to LEVELS [price10, quantity, orders], best price first.
BOOK_KEYS = {'bid_levels', 'ask_levels'}
LEVELS = 10
MASTER_KEYS = set('issue_id code tick_type base_price10 limit_up10 limit_down10 lot_size issue_type name'.split())
BATCH_KEYS = {
    'bootstrap': set('type seq source trading_date input_transport source_timestamp_origin exchange_delay_ms source_time_us market_issue_count master quotes'.split()),
    'quotes': set('type seq source_time_us received_unix_ms decode_ns replay_lateness_ms quotes'.split()),
    'end': set('type seq source_time_us frames quote_updates replay_wall_ms'.split()),
}
# Issues whose quote could not be read (the decoder's state for them was inconsistent) are listed
# instead, on the batches that would have carried them.
SKIPPED = 'skipped_issues'
SUMMARY_KEYS = {'source', 'trading_date', 'first_source_time_us', 'last_source_time_us', 'codes', 'batches', 'quote_updates', 'expanded_bytes'}
DEMO_ORIGIN = 'https://next-demo.brisk.jp/'
# Broker-hosted BRiSK (SBI, Tachibana e-shiten): the same app and decoder, so the same rules; each
# keeps only its fixed public transport provenance.
BROKER_TRANSPORT = {'sbi_live': {'kind': 'sbi_websocket', 'origin': 'https://sbi.brisk.jp/'},
                    'eshiten_live': {'kind': 'eshiten_websocket', 'origin': 'https://next.e-shiten.brisk.jp/'}}
SBI_TRANSPORT = BROKER_TRANSPORT['sbi_live']
REFERENCE_DIR = Path(__file__).resolve().parent / 'references'
# Local measurements are client reported. Plausible ranges and microsecond
# precision keep them useful as timing data and useless as a payload carrier.
MAX_DECODE_NS = 10 * 10**9
UNIX_MS = (1_600_000_000_000, 4_102_444_800_000)
MAX_SESSION_MS = 86_400_000
MAX_FETCH_MS = 600_000

def require(condition, message):
    if not condition:
        raise ValueError(message)

def integer(value, maximum=2**53-1):
    require(type(value) is int and 0 <= value <= maximum, 'Invalid unsigned integer')
    return value

def measurement(value, maximum):
    """Non-negative milliseconds with at most microsecond precision."""
    require(type(value) in (int, float) and 0 <= value <= maximum and math.copysign(1, value) > 0
            and round(value, 3) == value, 'Invalid local timing')
    return value

def code(value):
    require(isinstance(value, str) and re.fullmatch(r'[A-Z0-9]{4,8}', value), 'Invalid security code')
    return value

def book(levels, descending):
    """One side of a quote's book: up to LEVELS [price10, quantity, orders], best price first."""
    require(isinstance(levels, list) and len(levels) <= LEVELS, 'Invalid book side')
    for level in levels:
        require(isinstance(level, list) and len(level) == 3, 'Invalid book level')
        for value in level:
            integer(value)
    prices = [level[0] for level in levels]
    require(prices == sorted(set(prices), reverse=descending), 'Book prices out of order')

def _nonfinite(_):
    raise ValueError('Nonfinite JSON')

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)

def encode(batch):
    return (canonical(batch) + '\n').encode()

def _issue(item):
    require(isinstance(item, dict) and type(item.get('issue_id')) is int, 'Invalid issue entry')
    return item['issue_id']

def canonical_batches(source):
    """Decoder JSONL as canonical batches: issues sorted, timings in microseconds, public transport only."""
    while line := source.readline(MAX_LINE + 1):
        require(len(line) <= MAX_LINE, 'Recording exceeds decode limit')
        b = json.loads(line, parse_constant=_nonfinite)
        require(isinstance(b, dict), 'Invalid batch')
        public = BROKER_TRANSPORT.get(b.get('source')) if b.get('type') == 'bootstrap' else None
        if public:
            transport = b.get('input_transport')
            if isinstance(transport, dict) and all(transport.get(k) == v for k, v in public.items()):
                # Keep only public transport provenance, never session/profile or
                # decoder paths and connection diagnostics from the local host.
                b['input_transport'] = dict(public)
        for key in ('master', 'quotes'):
            if isinstance(b.get(key), list):
                b[key].sort(key=_issue)
        if isinstance(b.get(SKIPPED), list) and all(type(issue) is int for issue in b[SKIPPED]):
            b[SKIPPED].sort()
        for holder, key in ((b, 'replay_lateness_ms'), (b, 'replay_wall_ms'), (b.get('input_transport'), 'asset_fetch_ms')):
            if isinstance(holder, dict) and type(holder.get(key)) is float:
                holder[key] = round(holder[key], 3)
        yield b

def canonical_lines(source):
    """Decoder JSONL rewritten as canonical lines (see canonical_batches)."""
    return map(encode, canonical_batches(source))

class Fingerprint:
    """Digest of market content: the batch clock timeline plus one hash chain per issue."""
    def __init__(self):
        self.timeline = hashlib.sha256()
        self.issues = {}

    def batch(self, seq, now):
        self.timeline.update(b'%d:%d\n' % (seq, now))

    def master(self, entry):
        self.issues[entry['issue_id']] = hashlib.sha256(canonical(entry).encode())

    def quote(self, seq, quote):
        self.issues[quote['issue_id']].update(b'%d:' % seq + canonical(quote).encode() + b'\n')

    def digest(self, issue):
        return self.issues[issue].hexdigest()[:32]

def _transport(value, source):
    if source in BROKER_TRANSPORT:
        require(value == BROKER_TRANSPORT[source], 'Invalid broker transport fields')
        return
    require(value == {'kind': 'local_cache'} or (isinstance(value, dict) and set(value) == {'kind', 'origin', 'asset_fetch_ms'}
            and value['kind'] == 'https_recorded_assets' and value['origin'] == DEMO_ORIGIN), 'Invalid transport fields')
    if 'asset_fetch_ms' in value:
        measurement(value['asset_fetch_ms'], MAX_FETCH_MS)

def _lines(stream):
    """(batch, line) pairs from JSONL, reading no line beyond the decode limit."""
    while line := stream.readline(MAX_LINE + 1):
        require(len(line) <= MAX_LINE, 'Recording exceeds decode limit')
        yield json.loads(line, parse_constant=_nonfinite), line

def _scan(entries):
    """Check structure, canonical form and local timing; fingerprint market content.

    `entries` yields (batch, line): a JSONL line must be the batch's canonical encoding, and
    batches decoded from another format pass None.
    """
    identities, clocks = {}, {}
    fingerprint = Fingerprint()
    seq = last = count = expanded = updates = market = 0
    first = source = date = paced = received = previous_stamp = None
    ended = False
    for b, line in entries:
        require(isinstance(b, dict), 'Invalid batch')
        encoded = encode(b)
        # Exactly one byte encoding per value: no whitespace, duplicate keys,
        # escapes, key order or number spellings that could carry other data.
        require(line is None or encoded == line, 'Non-canonical encoding')
        expanded += len(encoded)
        require(len(encoded) <= MAX_LINE and expanded <= MAX_EXPANDED, 'Recording exceeds decode limit')
        require(not ended, 'Data after end batch')
        kind = b.get('type')
        require(kind in BATCH_KEYS and set(b) - {SKIPPED} == BATCH_KEYS[kind]
                and (SKIPPED not in b or kind != 'end'), 'Unknown or missing batch fields')
        require(integer(b['seq']) == seq, 'Sequence gap')
        now = integer(b['source_time_us'], 86_400_000_000 - 1)
        require(now >= last, 'Market clock regression')
        fingerprint.batch(seq, now)
        if seq == 0:
            require(kind == 'bootstrap' and isinstance(b['source'], str) and b['source'] in SOURCES,
                    'Unsupported bootstrap/source')
            source, date, first = b['source'], b['trading_date'], now
            require(isinstance(date, str) and re.fullmatch(r'\d{8}', date), 'Invalid date')
            dt.datetime.strptime(date, '%Y%m%d')
            master = b['master']
            require(isinstance(master, list) and 0 < len(master) <= 20000, 'Invalid master size')
            market = integer(b['market_issue_count'])
            require(market >= len(master), 'Invalid market count')
            issue = -1
            for m in master:
                require(isinstance(m, dict) and set(m) == MASTER_KEYS, 'Unknown or missing master fields')
                previous_issue, issue = issue, integer(m['issue_id'], 2**32-1)
                require(issue > previous_issue, 'Master identities must be unique and sorted')
                identities[issue] = code(m['code'])
                for key, value in m.items():
                    if key == 'name':
                        require(isinstance(value, str) and len(value) <= 256, 'Invalid security name')
                    elif key != 'code':
                        integer(value)
                fingerprint.master(m)
            # No account/session information is retained in the archive.
            _transport(b['input_transport'], source)
            require(b['source_timestamp_origin'] == 'brisk_decoder_unverified', 'Invalid clock provenance')
            require(b['exchange_delay_ms'] is None, 'Unverified exchange delay')
        else:
            require(kind in {'quotes', 'end'}, 'Expected delta/end')
        if kind == 'quotes':
            integer(b['decode_ns'], MAX_DECODE_NS)
            stamp = integer(b['received_unix_ms'])
            require(UNIX_MS[0] <= stamp < UNIX_MS[1], 'Implausible local receipt clock')
            if received is None:
                received = previous_stamp = stamp
            require(previous_stamp <= stamp <= received + MAX_SESSION_MS, 'Local receipt clock regression/span')
            previous_stamp = stamp
            lateness = b['replay_lateness_ms']
            if source in BROKER_TRANSPORT:
                require(lateness is None, 'Broker streams are not demo replays')
            if lateness is not None:
                measurement(lateness, MAX_SESSION_MS)
            # A replay is paced (measured) or unpaced (null) throughout.
            paced = (lateness is not None) if paced is None else paced
            require(paced == (lateness is not None), 'Inconsistent replay pacing')
        quotes = b.get('quotes', [])
        require(isinstance(quotes, list) and len(quotes) <= len(identities), 'Invalid quote count')
        previous_issue = -1
        for q in quotes:
            require(isinstance(q, dict) and set(q) == QUOTE_KEYS, 'Unknown or missing quote fields')
            issue = integer(q['issue_id'], 2**32-1)
            require(issue > previous_issue and identities.get(issue) == code(q['code']), 'Quote identity mismatch/duplicate')
            previous_issue = issue
            frame, clock = integer(q['frame'], 2**32-1), integer(q['source_time_us'], now)
            previous = clocks.get(issue, (0, 0))
            require(frame >= previous[0] and clock >= previous[1], 'Quote frame/time regression')
            clocks[issue] = (frame, clock)
            for key, value in q.items():
                if key in BOOK_KEYS:
                    book(value, descending=key == 'bid_levels')
                elif key != 'code':
                    integer(value)
            fingerprint.quote(seq, q)
        skipped = b.get(SKIPPED, [])
        require(isinstance(skipped, list) and skipped and all(issue in identities for issue in skipped)
                and skipped == sorted(set(skipped)) and not set(skipped) & {q['issue_id'] for q in quotes}
                if SKIPPED in b else True, 'Invalid skipped issues')
        if seq == 0:
            require(len(quotes) + len(skipped) == len(identities), 'Incomplete bootstrap')
        if kind == 'end':
            frames = integer(b['frames'])
            require((frames >= seq - 1 if source in BROKER_TRANSPORT else frames == seq)
                    and integer(b['quote_updates']) == updates, 'End summary mismatch')
            measurement(b['replay_wall_ms'], MAX_SESSION_MS)
            ended = True
        elif kind == 'quotes':
            updates += len(quotes)
        seq += 1
        last, count = now, count + len(quotes)
    require(ended, 'Recording has no clean end')
    summary = dict(source=source, trading_date=date, first_source_time_us=first,
                   last_source_time_us=last, codes=sorted(identities.values()), batches=seq,
                   quote_updates=count, expanded_bytes=expanded)
    return summary, fingerprint, market

def build_reference(stream):
    """Fingerprint a complete, trusted replay (canonical JSONL) of every issue in a source's market."""
    summary, fingerprint, market = _scan(_lines(stream))
    require(set(fingerprint.issues) == set(range(market)), 'Reference must cover the whole market')
    return dict(source=summary['source'], trading_date=summary['trading_date'], market_issue_count=market,
                batches=summary['batches'], timeline=fingerprint.timeline.hexdigest(),
                issues=[fingerprint.digest(i) for i in range(market)])

def synthetic_recording():
    """The only accepted synthetic_test recording: a one-issue publication probe."""
    master = {**dict.fromkeys(MASTER_KEYS, 0), 'issue_id': 0, 'code': '0000', 'lot_size': 100, 'name': 'Synthetic fixture'}
    quote = {**dict.fromkeys(QUOTE_KEYS, 0), 'issue_id': 0, 'code': '0000', 'frame': 1, 'max_frame': 1,
             'source_time_us': 100, 'indicative_price10': 1230, 'indicative_volume': 100, 'bid_levels': [], 'ask_levels': []}
    return [dict(type='bootstrap', seq=0, source='synthetic_test', trading_date='20210927', source_time_us=100,
                 input_transport={'kind': 'local_cache'}, source_timestamp_origin='brisk_decoder_unverified',
                 exchange_delay_ms=None, market_issue_count=1, master=[master], quotes=[quote]),
            dict(type='quotes', seq=1, source_time_us=110, received_unix_ms=1632700800000, decode_ns=1000,
                 replay_lateness_ms=0, quotes=[{**quote, 'frame': 2, 'max_frame': 2, 'source_time_us': 110,
                                                'indicative_price10': 1240}]),
            dict(type='end', seq=2, source_time_us=110, frames=2, quote_updates=1, replay_wall_ms=0.5)]

def load_references():
    references = {'synthetic_test': build_reference(io.BytesIO(b''.join(map(encode, synthetic_recording()))))}
    for path in sorted(REFERENCE_DIR.glob('*.json')):
        reference = json.loads(path.read_text())
        references[reference['source']] = reference
    return references

REFERENCES = load_references()

def validate_stream(stream, references=None):
    """Validate canonical JSONL (see validate_batches); every line must be its batch's canonical bytes."""
    return _verify(*_scan(_lines(stream)), references)

def validate_batches(batches, references=None):
    """Validate canonical fields/continuity, plus reference content for demo/synthetic data.

    Broker live content (SBI, e-shiten) is contributor-declared and has no reference
    replay. Its transport is exactly its BROKER_TRANSPORT, and replay_lateness_ms must
    be null. End frames include pre-bootstrap catch-up frames as well as the emitted
    quote batches. A batch lists in skipped_issues the issues whose quote could not be
    read; a bootstrap's quotes and skipped issues together cover its master.
    """
    return _verify(*_scan((b, None) for b in batches), references)

def _verify(summary, fingerprint, market, references):
    if summary['source'] in BROKER_TRANSPORT:
        return summary
    reference = (REFERENCES if references is None else references).get(summary['source'])
    require(reference is not None, 'No reference replay for this source')
    require(summary['trading_date'] == reference['trading_date'] and market == reference['market_issue_count']
            and summary['batches'] == reference['batches'] and fingerprint.timeline.hexdigest() == reference['timeline'],
            'Recording is not a complete reference replay')
    issues = reference['issues']
    require(all(i < len(issues) and fingerprint.digest(i) == issues[i] for i in fingerprint.issues),
            'Market data differs from the reference replay')
    return summary

def validate_manifest(m):
    fields = {'schema', 'sha256', 'bytes', 'summary', 'contributor', 'license', 'redistribution_permitted'}
    require(isinstance(m, dict) and set(m) == fields, 'Invalid manifest fields')
    require(m['schema'] == SCHEMA and isinstance(m['sha256'], str) and re.fullmatch('[0-9a-f]{64}', m['sha256']), 'Invalid schema/hash')
    require(0 < integer(m['bytes'], MAX_COMPRESSED), 'Empty upload')
    require(isinstance(m['contributor'], str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', m['contributor']), 'Invalid contributor alias')
    require(isinstance(m['license'], str) and m['license'] in {'CC0-1.0', 'CC-BY-4.0'}
            and m['redistribution_permitted'] is True, 'Redistribution declaration required')
    summary = m['summary']
    require(isinstance(summary, dict) and set(summary) == SUMMARY_KEYS and isinstance(summary['source'], str)
            and summary['source'] in SOURCES, 'Invalid summary')
    require(isinstance(summary['trading_date'], str) and re.fullmatch(r'\d{8}', summary['trading_date']), 'Invalid summary date')
    dt.datetime.strptime(summary['trading_date'], '%Y%m%d')
    for key in SUMMARY_KEYS - {'source', 'trading_date', 'codes'}:
        integer(summary[key])
    codes = summary['codes']
    require(isinstance(codes, list) and 0 < len(codes) <= 20000, 'Invalid summary codes')
    for value in codes:
        code(value)
    require(codes == sorted(set(codes)), 'Invalid summary code order/duplicates')
    first, last = summary['first_source_time_us'], summary['last_source_time_us']
    require(first <= last < 86_400_000_000 and summary['batches'] >= 2
            and summary['quote_updates'] >= len(codes) and summary['expanded_bytes'] > 0, 'Invalid summary bounds')
    reference = REFERENCES.get(summary['source'])
    if reference:
        require(summary['trading_date'] == reference['trading_date'] and summary['batches'] == reference['batches'],
                'Summary is not a complete reference replay')
    return m

# Timing reports: what a broker session (SBI, e-shiten) contributes besides any opted-in market data.
TIMING_SCHEMA = 'brisk-timing-v1'
TIMING_SOURCES = set(BROKER_TRANSPORT)
TIMING_KEYS = {'schema', 'source', 'trading_date', 'first_minute', 'last_minute', 'frames', 'stalls',
               'client_version', 'decode_ms', 'source_age_ms', 'interarrival_ms', 'contributor', 'license'}
QUANTILES = ('p50', 'p90', 'p99', 'max')
# Bounds per distribution (ms). Source age can be negative when local and feed clocks disagree.
TIMING_BOUNDS = {'decode_ms': (0, 10_000), 'source_age_ms': (-600_000, 600_000), 'interarrival_ms': (0, 3_600_000)}
MAX_TIMING_BYTES = 2048
MIN_TIMING_FRAMES = 100

def validate_timing(report, today=None):
    """A timing report: exact fields, bounded microsecond-precision distributions, no market data.

    `today` (a date) additionally requires a trading date within the last 30 days.
    """
    require(isinstance(report, dict) and set(report) == TIMING_KEYS, 'Invalid timing report fields')
    require(report['schema'] == TIMING_SCHEMA and report['source'] in TIMING_SOURCES, 'Invalid timing schema/source')
    date = report['trading_date']
    require(isinstance(date, str) and re.fullmatch(r'\d{8}', date), 'Invalid timing date')
    day = dt.datetime.strptime(date, '%Y%m%d').date()
    if today is not None:
        require(0 <= (today - day).days <= 30, 'Timing report date out of range')
    minutes = [report['first_minute'], report['last_minute']]
    require(all(isinstance(m, str) and re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d', m) for m in minutes)
            and minutes[0] <= minutes[1], 'Invalid session minutes')
    frames = integer(report['frames'], 10_000_000)
    require(frames >= MIN_TIMING_FRAMES and integer(report['stalls']) <= frames, 'Invalid frame counts')
    require(isinstance(report['client_version'], str) and re.fullmatch(r'\d{1,4}\.\d{1,4}\.\d{1,4}', report['client_version']),
            'Invalid client version')
    require(isinstance(report['contributor'], str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', report['contributor']),
            'Invalid contributor alias')
    require(report['license'] in {'CC0-1.0', 'CC-BY-4.0'}, 'Invalid license')
    for name, (low, high) in TIMING_BOUNDS.items():
        values = report[name]
        require(isinstance(values, dict) and set(values) == set(QUANTILES), f'Invalid {name}')
        series = [values[q] for q in QUANTILES]
        require(all(type(v) in (int, float) and low <= v <= high and round(v, 3) == v for v in series)
                and series == sorted(series), f'Invalid {name}')
    require(len(encode(report)) <= MAX_TIMING_BYTES, 'Timing report too large')
    return report
