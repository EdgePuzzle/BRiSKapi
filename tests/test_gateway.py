"""briskapi.Live and the feed's gateway: Databento-style records from the feed on this computer, the same as
the cloud API's, served on 127.0.0.1 to holders of the gateway's key only."""
import collections
import datetime as dt
import io
import json
import os
from pathlib import Path
import socket
import threading
import time

import pytest

import briskapi as brisk
import briskapi.cli as cli
from briskapi import _gateway, _live, records
from test_api import market
from test_query import OPEN_US, OWNER, archive, at, isolated, query, session, trades_at  # noqa: F401


@pytest.fixture(autouse=True)
def nothing_loaded(monkeypatch):
    monkeypatch.setattr(brisk, '_current', None)


@pytest.fixture
def decoder(tmp_path):
    """A fake decoder printing these batches, waiting before each line index in `holds` until release(index)."""
    made = []

    def make(batches, holds=()):
        script = tmp_path / f'decoder{len(made)}.cjs'
        made.append(script)
        script.write_text(
            f"const fs = require('fs'); const lines = {json.dumps([json.dumps(b) for b in batches])};\n"
            f"const holds = {json.dumps(sorted(holds))}; const gate = {json.dumps(str(tmp_path / 'gate-'))}; let at = 0;\n"
            "const go = () => { while (at < lines.length) {\n"
            "  if (holds.includes(at) && !fs.existsSync(gate + at)) return setTimeout(go, 5);\n"
            "  console.log(lines[at++]); } };\n"
            "go();\n")
        return script, lambda index: (tmp_path / f'gate-{index}').touch()
    return make


def wait_for(condition, timeout=20):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, 'timed out'
        time.sleep(0.01)


def broker_feed(script, **options):
    """A broker-style feed (dataset XTKS.BRISK) from a fake decoder, serving briskapi.Live."""
    return _live.Feed(command=['node', str(script)], upload=False, gateway=True, **options).ready()


def test_live_records_are_the_cloud_apis(archive, decoder):
    """Replayed from the day's start, then live: every schema's records, bars included, equal the cloud API's for
    that session."""
    s3, _, put = archive
    batches = session(0, 150)
    put(batches)
    script, release = decoder(batches, holds=[70])
    feed = broker_feed(script)
    wait_for(lambda: feed.seq == 69)
    whole, part = brisk.Live(), brisk.Live()
    for schema in records.SCHEMAS:
        whole.subscribe(dataset='XTKS.BRISK', schema=schema, start=0)
    part.subscribe(dataset='XTKS.BRISK', schema='trades', symbols=['167A'], start=at(25))
    got = collections.defaultdict(list)
    whole.add_callback(lambda r: got[r.schema].append(dict(r)))
    whole.start()
    later = []
    reader = threading.Thread(target=lambda: later.extend(part))
    reader.start()
    # Both subscriptions answered before the feed goes on: one that came after the day's end would get nothing.
    wait_for(lambda: len(feed.gateway._clients) == 2 and all(c.known is not None for c in feed.gateway._clients))
    release(70)
    whole.block_for_close(timeout=30)
    reader.join(30)
    feed.wait()
    for schema in records.SCHEMAS:
        expected = query(s3, OWNER, dataset='XTKS.BRISK', schema=schema, start='2026-10-13').to_pylist()
        assert (expected or schema in {'ohlcv-1h', 'ohlcv-1d'}) and got[schema] == expected, schema
    assert [bar['ts_event'].minute for bar in got['ohlcv-1m']] == [0, 0, 1, 1]   # 09:02 is not covered
    expected = query(s3, OWNER, dataset='XTKS.BRISK', schema='trades', symbols='167A', start=at(25)).to_pylist()
    assert expected and [dict(r) for r in later] == expected
    assert later[0].schema == 'trades' and later[0].price == later[0]['price'] and later[0].symbol == '167A'
    assert not whole.is_connected() and 'closed' in repr(whole) and list(whole) == []
    assert whole.symbology_map == {7203: '7203', 154694: '167A'}


def test_a_session_joining_later_starts_from_now(decoder):
    """Without start: the day's definitions, the current quotes if asked (as a snapshot), then what arrives."""
    batches = market()
    script, release = decoder(batches, holds=[2])
    feed = _live.Feed(command=['node', str(script)], upload=False, gateway=True).ready()
    wait_for(lambda: feed.seq == 1)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'definition', symbols='7203')
    live.subscribe('XTKS.BRISK', 'mbp-10', symbols=['7203'], snapshot=True)
    live.subscribe('XTKS.BRISK', 'trades')
    live.start()
    release(2)
    found = list(live)
    feed.wait()
    assert [r.schema for r in found] == ['definition', 'mbp-10', 'trades', 'trades', 'mbp-10']
    definition, snapshot, *trades, update = found
    assert definition.raw_symbol == '7203' and definition.high_limit_price == 13100.0
    # The snapshot is 7203's quote as it stood (frame 2), flagged as one; then only what came after joining.
    assert (snapshot.flags, snapshot.sequence, snapshot.bid_px_00) == (32, 2, None)
    assert [t.size for t in trades] == [2000, 3300] and [t.trade_id for t in trades] == ['3-0', '3-1']
    assert (update.flags, update.sequence) == (0, 3)
    # Broker records carry the local receipt time.
    assert update.ts_recv == dt.datetime.fromtimestamp(1632700800, dt.timezone.utc) != update.ts_event


def test_subscriptions_join_a_running_session(decoder):
    """Subscribing during a session: new symbols, a replay of the day for that subscription alone, and bars that
    begin once it has joined."""
    batches = session(0, 200)
    script, release = decoder(batches, holds=[1, 100])
    feed = broker_feed(script)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'trades', symbols='7203')
    got = collections.defaultdict(list)
    live.add_callback(lambda r: got[r.schema, r.symbol].append(r))
    live.start()
    wait_for(lambda: live._subscriptions[0].active)
    release(1)
    wait_for(lambda: len(got['trades', '7203']) == 9)
    live.subscribe('XTKS.BRISK', 'trades', symbols='167A', start=0)       # the day so far, for 167A alone
    live.subscribe('XTKS.BRISK', 'ohlcv-1m', symbols='7203')             # bars beginning after it joined
    live.subscribe('XTKS.BRISK', 'imbalance', symbols='167A', snapshot=True)
    release(100)
    live.block_for_close(timeout=30)
    feed.wait()
    assert [t.trade_id for t in got['trades', '167A']] == [f'{s + 1}-0' for s in range(30, 201, 30)]
    assert [t.sequence for t in got['trades', '7203']] == [s + 1 for s in range(10, 201, 10)]   # each once
    # Joined at 09:01:40: the 09:02 bar is the first it covers, and 09:03 is not, ending at 09:03:20.
    minute = [(price10 / 10, size) for s in range(120, 180) for i, price10, size in trades_at(s) if i == 0]
    assert [(b.ts_event.minute, b.open, b.close, b.volume) for b in got['ohlcv-1m', '7203']] == \
        [(2, minute[0][0], minute[-1][0], sum(size for _, size in minute))]
    assert got['imbalance', '167A'] == []    # no auction after the open; the snapshot's quote has none either
    assert live.symbology_map[154694] == '167A'


def test_a_quiet_bar_closes_on_this_computers_clock(decoder, monkeypatch):
    """No later batch need say a bar has ended: this computer's clock, less the feed's delay, does."""
    monkeypatch.setattr(_gateway, 'LATENCY_WINDOW_NS', 0)    # allow for the latest delay only
    script, release = decoder(session(0, 30), holds=[1, 11])
    feed = broker_feed(script)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'ohlcv-1s', symbols='7203')
    bars = []
    live.add_callback(bars.append)
    live.start()
    wait_for(lambda: live._subscriptions[0].active)
    release(1)      # up to 09:00:10, whose trade opens the 09:00:10 bar; then the feed is quiet
    wait_for(lambda: bars, timeout=5)
    assert feed.seq == 10 and [(b.ts_event.second, b.open, b.volume) for b in bars] == [(10, 3001.0, 200)]
    release(11)
    live.block_for_close(timeout=30)
    feed.wait()
    assert [b.ts_event.second for b in bars] == [10, 20]     # 09:00:30's bar ends after the session does


def test_a_late_trade_goes_into_the_next_open_bar(decoder):
    """A trade stamped 09:00:19.5 that arrives with the 09:00:21 batch: its own bar has ended, so it counts in the
    next one still open (09:00:20), whose open it is by time."""
    batches = session(0, 30)
    late = {'issue_id': 0, 'code': '7203', 'frame': 22, 'time_us': OPEN_US + 19_500_000, 'price10': 31000,
            'quantity': 100, 'side': 1}
    batches[21] = dict(batches[21], trades=[*batches[21]['trades'], late])
    script, release = decoder(batches, holds=[1])
    feed = broker_feed(script)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'ohlcv-1s', symbols='7203')
    live.start()
    wait_for(lambda: live._subscriptions[0].active)
    release(1)
    bars = [(b.ts_event.second, b.open, b.high, b.low, b.close, b.volume) for b in live]
    feed.wait()
    assert bars == [(10, 3001.0, 3001.0, 3001.0, 3001.0, 200), (20, 3100.0, 3100.0, 3002.0, 3002.0, 400)]


def test_one_session_spans_trading_days(decoder):
    batches = session(0, 10) + session(0, 12, date='20261014')
    script, release = decoder(batches, holds=[5])
    feed = broker_feed(script)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'definition')
    live.subscribe('XTKS.BRISK', 'trades', symbols='7203')
    live.start()
    release(5)
    found = list(live)
    feed.wait()
    days = collections.Counter((r.schema, r.ts_event.date().isoformat()) for r in found)
    assert days == {('definition', '2026-10-12'): 2, ('trades', '2026-10-13'): 1,
                    ('definition', '2026-10-13'): 2, ('trades', '2026-10-14'): 1}
    assert len(feed.recordings) == 2 and feed.recordings[0]['name'] is None   # served, not packaged


def test_the_demo_through_connect_and_the_cli(decoder, monkeypatch, capsys):
    script, release = decoder(market(), holds=[1])
    monkeypatch.setattr(_live, 'DECODER', script)
    monkeypatch.setattr(cli, 'package_parts', lambda *a, **k: 1 / 0)    # the gateway alone packages nothing
    errors = []
    runner = threading.Thread(target=lambda: errors.append(cli.main(['live', '--site', 'demo', '--cache', '.', '--no-upload'])))
    runner.start()
    wait_for(lambda: _gateway.gateway_path('XTKS.BRISKDEMO').exists())
    live = brisk.Live()
    live.subscribe('XTKS.BRISKDEMO', 'imbalance', symbols='7203', start='2021-09-27')
    live.subscribe('XTKS.BRISKDEMO', 'trades', symbols='7203', start='2021-09-27T09:00:00.00002')
    out = io.StringIO()
    live.add_stream(out)
    live.start()
    release(1)
    live.block_for_close(timeout=30)
    runner.join(30)
    assert errors == [None]
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    assert [line['symbol'] for line in lines] == ['7203'] * 3
    assert lines[0]['ind_match_price'] == 10150.0 and lines[0]['ts_recv'] == lines[0]['ts_event']   # the demo: BRiSK's stamp
    assert [line['size'] for line in lines if 'trade_id' in line] == [2000, 3300]
    printed = capsys.readouterr()
    assert printed.out == '' and 'Serving XTKS.BRISKDEMO to briskapi.Live() on this computer only (127.0.0.1:' in printed.err
    assert not _gateway.gateway_path('XTKS.BRISKDEMO').exists()

    script, release = decoder(market())
    monkeypatch.setattr(_live, 'DECODER', script)
    cli.main(['live', '--site', 'demo', '--cache', '.', '--no-gateway', '--no-upload', '--quotes'])
    printed = capsys.readouterr()
    assert 'briskapi.Live' not in printed.err and printed.out


def test_the_gateway_admits_only_its_key_on_this_computer():
    gateway = _gateway.Gateway('XTKS.BRISK')
    info = json.loads(gateway.path.read_text())
    assert gateway._server.getsockname()[0] == '127.0.0.1' and info['port'] == gateway.port and info['key'] == gateway.key
    if os.name == 'posix':
        assert gateway.path.stat().st_mode & 0o777 == 0o600

    def connect(*messages, port=gateway.port):
        sock = socket.create_connection(('127.0.0.1', port), timeout=5)
        for message in messages:
            sock.sendall(message if isinstance(message, bytes) else json.dumps(message).encode() + b'\n')
        return sock, sock.makefile('rb')

    def ask(*messages, port=gateway.port, lines=1):
        sock, reader = connect(*messages, port=port)
        with sock:
            answers = [json.loads(reader.readline()) for _ in range(lines)]
        return answers[0] if lines == 1 else answers
    good = {'version': 1, 'key': gateway.key, 'dataset': 'XTKS.BRISK'}
    assert ask({**good, 'key': 'guess'}) == {'gateway': 'error', 'error': 'Wrong key'}
    assert ask({**good, 'version': 2})['error'].startswith('This gateway speaks version 1')
    assert ask({**good, 'dataset': 'XTKS.BRISKDEMO'})['error'] == 'This gateway serves XTKS.BRISK'
    assert ask(b'not json\n') == {'gateway': 'error', 'error': 'This gateway speaks version 1; update briskapi'}
    assert ask({**good, 'probe': True}) == {'gateway': 'probe', 'dataset': 'XTKS.BRISK'}
    assert ask(good) == {'gateway': 'session', 'version': 1, 'dataset': 'XTKS.BRISK'}
    assert ask(good, {'subscribe': [0], 'symbols': ['../x']}, lines=2)[1] == {'gateway': 'error', 'error': "Invalid symbol '../x'"}
    assert ask(good, {'subscribe': 'all'}, lines=2)[1]['error'] == 'Expected {"subscribe": [ids], ...}'
    # Before the feed's first batch: nothing to give yet, and no replay.
    assert ask(good, {'subscribe': [0], 'symbols': None, 'replay': True, 'snapshot': True}, lines=2)[1] == \
        {'gateway': 'subscribed', 'id': [0], 'bootstrap': None, 'snapshot': None, 'replay': False}
    held, reader = connect(good)
    assert json.loads(reader.readline())['gateway'] == 'session'
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(_gateway, 'MAX_CLIENTS', 1)
        wait_for(lambda: len(gateway._clients) == 1)
        assert ask(good)['error'] == 'At most 1 Live sessions at once'
    held.close()
    with pytest.raises(brisk.BriskError, match='Wrong key'):
        live = brisk.Live(key='guess', port=gateway.port)
        live.subscribe('XTKS.BRISK', 'trades')
        live.start()
    # One gateway per dataset: another feed is refused while this one answers.
    with pytest.raises(brisk.BriskError, match='already serves XTKS.BRISK'):
        _gateway.Gateway('XTKS.BRISK')
    gateway.close()
    assert not gateway.path.exists()
    with pytest.raises(brisk.BriskError, match=r'No feed on this computer serves XTKS.BRISK: start one with `brisk live --site sbi`'):
        brisk.Live().subscribe('XTKS.BRISK', 'trades')
    # A file left by a feed that died is replaced, and a session finding no one there says so.
    stale = dict(info, port=_free_port())
    gateway.path.write_text(json.dumps(stale))
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'trades')
    with pytest.raises(brisk.BriskError, match='is `brisk live` still running'):
        live.start()
    replaced = _gateway.Gateway('XTKS.BRISK')
    assert json.loads(replaced.path.read_text())['key'] == replaced.key != stale['key']
    replaced.close('completed')
    assert ask({'version': 1, 'key': replaced.key, 'dataset': 'XTKS.BRISK'}, port=replaced.port)['status'] == 'completed'
    wait_for(lambda: replaced._server.fileno() == -1)     # and soon stops listening


def _free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def test_a_slow_session_is_dropped_not_waited_for(monkeypatch):
    monkeypatch.setattr(_gateway, 'MAX_BEHIND', 4 * 1024**2)
    gateway = _gateway.Gateway('XTKS.BRISKDEMO')
    sock = socket.create_connection(('127.0.0.1', gateway.port))
    sock.sendall(json.dumps({'version': 1, 'key': gateway.key, 'dataset': 'XTKS.BRISKDEMO'}).encode() + b'\n'
                 + json.dumps({'subscribe': [0], 'symbols': None}).encode() + b'\n')
    wait_for(lambda: gateway._clients and gateway._clients[0].codes is None)
    line = b'{"type":"quotes"' + b' ' * 1024**2 + b'}\n'
    client = gateway._clients[0]   # it may leave the list as soon as it is dropped
    started = time.monotonic()
    for _ in range(64):          # 64 MB to a session that reads nothing
        gateway.publish({'type': 'quotes', 'quotes': []}, line)
    assert time.monotonic() - started < 5 and client._last   # the feed went on; the session is dropped
    received = b''.join(iter(lambda: sock.recv(1 << 20), b''))
    assert json.loads(received.splitlines()[-1])['error'].startswith('This session fell more than 4 MB behind')
    wait_for(lambda: not gateway._clients)
    gateway.close()
    sock.close()


def test_live_api_errors_and_controls(decoder, tmp_path):
    script, release = decoder(market(), holds=[1, 3])
    feed = broker_feed(script)
    live = brisk.Live()
    for args, message in ((('XTKS.NOPE', 'trades'), 'Unknown dataset'), (('XTKS.BRISK', 'tbbo'), 'Unknown schema'),
                          (('XTKS.BRISK', 'trades', 'ALL_SYMBOLS', 'isin'), 'raw_symbol')):
        with pytest.raises(ValueError, match=message):
            live.subscribe(*args)
    with pytest.raises(ValueError, match='not both'):
        live.subscribe('XTKS.BRISK', 'mbp-10', start=0, snapshot=True)
    with pytest.raises(ValueError, match='snapshot applies'):
        live.subscribe('XTKS.BRISK', 'trades', snapshot=True)
    with pytest.raises(brisk.BriskError, match='Subscribe to something'):
        live.start()
    with pytest.raises(brisk.BriskError, match='not started'):
        live.block_for_close()
    live.subscribe('XTKS.BRISK', 'brisk-quotes', symbols=['7203'])
    with pytest.raises(ValueError, match='streams XTKS.BRISK'):
        live.subscribe('XTKS.BRISKDEMO', 'trades')
    failures, written = [], tmp_path / 'records.jsonl'
    live.add_callback(lambda r: 1 / 0, failures.append)
    live.add_callback(lambda r: 1 / 0)
    live.add_stream(written)
    with pytest.raises(FileExistsError):
        live.add_stream(written)
    with pytest.warns(UserWarning, match='Live callback failed: ZeroDivisionError'):
        live.start()
        with pytest.raises(brisk.BriskError, match='streams once'):
            live.start()
        wait_for(lambda: live._subscriptions[0].active)
        release(1)
        wait_for(lambda: written.read_text().count('\n') == 1)
    assert len(failures) == 1 and live.is_connected() and 'streaming' in repr(live)
    live.block_for_close(timeout=0.1)        # stops it: the feed goes on
    assert not live.is_connected() and feed.status == 'running'
    assert [json.loads(line)['sequence'] for line in written.read_text().splitlines()] == [2]
    with pytest.raises(brisk.BriskError, match='has ended'):
        live.subscribe('XTKS.BRISK', 'trades')
    release(3)
    feed.wait()


def test_a_failed_feed_fails_its_sessions(decoder):
    broken = market()[:3]
    broken[2] = dict(broken[2], seq=9)
    script, release = decoder(broken, holds=[2])
    feed = broker_feed(script)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'trades')
    records_seen = []
    live.start()
    release(2)
    with pytest.raises(brisk.BriskError, match='The feed ended the session: Stream sequence error at 9'):
        records_seen.extend(live)
    with pytest.raises(brisk.BriskError, match='sequence error'):
        feed.wait()
    with pytest.raises(brisk.BriskError, match='sequence error'):
        live.block_for_close()


def test_a_session_whose_feed_vanishes_fails():
    gateway = _gateway.Gateway('XTKS.BRISKDEMO')
    live = brisk.Live()
    live.subscribe('XTKS.BRISKDEMO', 'trades')
    live.start()
    wait_for(lambda: gateway._clients)
    gateway._clients[0]._stop()       # as when the feed's process is killed
    with pytest.raises(brisk.BriskError, match='closed the connection without ending the session'):
        list(live)
    gateway.close()


def test_records_read_as_attributes():
    record = brisk.Record({'price': 1.5}, 'trades')
    assert record.price == record['price'] == 1.5 and record == {'price': 1.5} and record.schema == 'trades'
    assert repr(record) == "Record('trades', {'price': 1.5})"
    with pytest.raises(AttributeError):
        record.size


def _fast(batch, codes):
    """A frame's batch as a broker host's fast lane writes it: only these symbols' quotes and trades."""
    return {**{k: v for k, v in batch.items() if k not in ('quotes', 'trades')}, 'lane': 'fast',
            'quotes': [q for q in batch['quotes'] if q['code'] in codes],
            'trades': [t for t in batch['trades'] if t['code'] in codes]}


def _line_of(batch):
    return json.dumps(batch).encode() + b'\n'


def _records(schemas, symbols):
    """A Live session's records, as dicts, by schema and symbol."""
    live, got = brisk.Live(), collections.defaultdict(list)
    for schema in schemas:
        live.subscribe('XTKS.BRISK', schema, symbols=symbols)
    live.add_callback(lambda r: got[r.schema, r.symbol].append(dict(r)))
    live.start()
    return live, got


def test_a_session_on_the_fast_lane_gets_its_symbols_ahead_of_the_recording_and_the_same_records():
    batches = session(0, 60)
    told = []
    gateway = _gateway.Gateway('XTKS.BRISK', on_fast=told.append)
    fast, fast_got = _records(['trades', 'mbp-10'], ['7203'])   # before the day's session: waiting to know its lanes
    slow, slow_got = _records(['trades', 'mbp-10'], 'ALL_SYMBOLS')  # never on the fast lane
    wait_for(lambda: sorted(c.lane for c in gateway._clients) == ['pending', 'slow'])
    assert told[-1] == {'7203'}, 'the host is told which symbols the fast lane reads'
    gateway.lane({'type': 'lanes', 'fast': True, 'capacity': 500, 'codes': ['7203']}, None)
    assert sorted(c.lane for c in gateway._clients) == ['fast', 'slow']
    gateway.publish(batches[0], _line_of(batches[0]))
    # The whole day on the fast lane before any of the recording's batches (which wait behind the whole market).
    for b in batches[1:-1]:
        gateway.lane(_fast(b, {'7203'}), None)
    wait_for(lambda: len(fast_got['trades', '7203']) == 6 and len(fast_got['mbp-10', '7203']) == 61)   # with the bootstrap's
    assert not slow_got['trades', '7203'], 'nothing yet on the recording'
    for b in batches[1:]:
        gateway.publish(b, _line_of(b))
    wait_for(lambda: len(slow_got['mbp-10', '7203']) == 61)
    gateway.close()
    fast.block_for_close(timeout=10)
    slow.block_for_close(timeout=10)
    assert fast_got['trades', '7203'] == slow_got['trades', '7203'] and len(fast_got['trades', '7203']) == 6
    assert fast_got['mbp-10', '7203'] == slow_got['mbp-10', '7203'], 'each record once, the same as the recording\'s'
    assert not any(symbol == '167A' for _, symbol in fast_got)


def test_a_session_leaving_the_fast_lane_or_joining_it_a_symbol_misses_nothing_and_gets_nothing_twice():
    batches = session(0, 60)
    told = []
    gateway = _gateway.Gateway('XTKS.BRISK', on_fast=told.append)
    gateway.lane({'type': 'lanes', 'fast': True, 'capacity': 2, 'codes': []}, None)
    gateway.publish(batches[0], _line_of(batches[0]))
    joins, joins_got = _records(['mbp-10'], ['7203'])
    leaves, leaves_got = _records(['mbp-10'], ['7203'])
    reference, reference_got = _records(['mbp-10'], 'ALL_SYMBOLS')
    wait_for(lambda: len(gateway._clients) == 3 and sum(c.lane == 'joining' for c in gateway._clients) == 2
             and any(c.lane == 'slow' and c.codes is None for c in gateway._clients))
    gateway.lane({'lane': 'fast', 'type': 'join', 'seq': 0, 'quotes': [], 'codes': ['7203']}, None)
    wait_for(lambda: sorted(c.lane for c in gateway._clients) == ['fast', 'fast', 'slow'])
    # The fast lane is at batch 30, the recording at 10.
    for b in batches[1:11]:
        gateway.publish(b, _line_of(b))
    for b in batches[1:31]:
        gateway.lane(_fast(b, {'7203'}), None)
    wait_for(lambda: len(joins_got['mbp-10', '7203']) == 30 and len(leaves_got['mbp-10', '7203']) == 30)
    # One adds 167A, which fits (two symbols); the other asks for a replay of it, which the fast lane has not.
    joining, leaving = [c for c in gateway._clients if c.lane == 'fast']
    joins.subscribe('XTKS.BRISK', 'mbp-10', symbols=['167A'])
    wait_for(lambda: joining.lane == 'joining' or leaving.lane == 'joining')
    if leaving.lane == 'joining':
        joining, leaving = leaving, joining
    assert told[-1] == {'7203', '167A'}
    leaves.subscribe('XTKS.BRISK', 'mbp-10', symbols=['167A'], start=0)
    wait_for(lambda: leaving.lane == 'slow')
    assert (leaving.fast_until, leaving.old_codes) == (30, {'7203'})
    gateway.lane({'lane': 'fast', 'type': 'join', 'seq': 30, 'quotes': [batches[30]['quotes'][1]], 'codes': ['167A']}, None)
    wait_for(lambda: joining.lane == 'fast')
    for b in batches[31:-1]:
        gateway.lane(_fast(b, {'7203', '167A'}), None)
    for b in batches[11:]:
        gateway.publish(b, _line_of(b))
    wait_for(lambda: len(reference_got['mbp-10', '7203']) == 60)   # joined after the bootstrap
    gateway.close()
    for live in (joins, leaves, reference):
        live.block_for_close(timeout=10)
    # 7203 throughout, from either lane, exactly as the recording has it.
    assert joins_got['mbp-10', '7203'] == leaves_got['mbp-10', '7203'] == reference_got['mbp-10', '7203']
    # 167A: on the fast lane after its join; from the recording after the subscription for the one that left.
    after = lambda records, seq: [r for r in records if r['sequence'] > seq + 1]  # noqa: E731 - sequence is the frame
    assert joins_got['mbp-10', '167A'] == after(reference_got['mbp-10', '167A'], 30)
    assert leaves_got['mbp-10', '167A'] == after(reference_got['mbp-10', '167A'], 10)


HOST = """'use strict';
// The live host against a fake broker site serving the demo's frames, a few at a time, with its fast lane told
// on stdin, as the Python feed runs it.
const fs = require('fs'), path = require('path');
const { once } = require('events');
const { live, readLanes } = require(%(brisk)s);
const { frames } = require(%(decoder)s);
const cache = process.env.BRISK_MOCK_CACHE;
const files = Object.fromEntries(['fita.js', 'fita.wasm', 'master.dat', 'snapshot.dat', 'ws.dat'].map(n => [n, fs.readFileSync(path.join(cache, n))]));
const chunks = [...frames(files['ws.dat'])].slice(0, 600).map(f => f.data);
const routes = {
  '/api/frontend/boot': () => JSON.stringify({ api_token: 't', identity: 'a'.repeat(64) }),
  '/api/app/boot': () => JSON.stringify({ date: '2021-09-27', series: 0, master: 'm1', snapshot: 's1', ws_url: '/realtime/0?session=abc' }),
  '/': () => '<html><script src="/main.abc.js"></script></html>',
  '/main.abc.js': () => 'load("assets/wasm/fita.test.js");load("assets/wasm/fita.test.wasm")',
  '/assets/wasm/fita.test.js': () => files['fita.js'], '/assets/wasm/fita.test.wasm': () => files['fita.wasm'],
  '/api/master/m1': () => files['master.dat'], '/api/snapshot/s1': () => files['snapshot.dat'],
};
const fetchImpl = async url => new Response(routes[url.pathname] ? routes[url.pathname]() : '', { status: routes[url.pathname] ? 200 : 404 });
class Socket extends EventTarget {
  constructor() {
    super();
    let i = 0;
    const send = n => { for (const end = Math.min(chunks.length, i + n); i < end; i++) {
      const c = chunks[i]; this.dispatchEvent(new MessageEvent('message', { data: c.buffer.slice(c.byteOffset, c.byteOffset + c.length) })); } };
    setImmediate(() => send(3));
    this.timer = setInterval(() => { send(5); if (i >= chunks.length) { clearInterval(this.timer); setTimeout(() => this.close(1000), 50); } }, 25);
  }
  send() {}
  close(code = 1000) { clearInterval(this.timer); if (this.closed) return; this.closed = true; this.dispatchEvent(Object.assign(new Event('close'), { code, reason: '' })); }
}
live({ cookies: { s: 'v' }, protocolVersion: 16000, fetchImpl, WebSocketImpl: Socket, staleSeconds: 0, heartbeatMs: 1e9,
  lanes: readLanes(process.stdin),
  emit: async (record, line = JSON.stringify(record)) => { if (!process.stdout.write(line + '\\n')) await once(process.stdout, 'drain'); } })
  // Exit once stdout has taken every line: on macOS a pipe is written asynchronously, and exiting at once can cut
  // the last lines short.
  .then(() => process.stdout.write('', () => process.exit(0)), error => { console.error(error.message); process.exit(1); });
"""


@pytest.mark.skipif(not os.environ.get('BRISK_MOCK_CACHE'), reason='set BRISK_MOCK_CACHE for fixture replay')
def test_a_live_session_joins_the_fast_lane_of_a_running_host_through_the_feed(tmp_path):
    decoders = Path(_live.__file__).parent / 'decoder'
    script = tmp_path / 'host.cjs'
    script.write_text(HOST % {'brisk': json.dumps(str(decoders / 'brisk.cjs')), 'decoder': json.dumps(str(decoders / 'decoder.cjs'))})
    feed = _live.Feed(command=['node', str(script)], env={**os.environ, 'BRISK_WORKERS': '3'}, upload=False, gateway=True).ready()
    assert feed.gateway._lanes == {'fast': True, 'capacity': 500}
    slow, slow_got = _records(['mbp-10'], 'ALL_SYMBOLS')
    wait_for(lambda: [c.lane for c in feed.gateway._clients] == ['slow'] and feed.gateway._clients[0].codes is None)
    fast, fast_got = _records(['mbp-10', 'trades'], ['7203'])
    wait_for(lambda: sorted(c.lane for c in feed.gateway._clients) == ['fast', 'slow'])   # joined: the host read it
    feed.wait()
    fast.block_for_close(timeout=30)
    slow.block_for_close(timeout=30)
    got, expected = fast_got['mbp-10', '7203'], slow_got['mbp-10', '7203']
    assert len(got) > 20 and got == expected[-len(got):], 'from its join, the recording\'s records, each once'
    assert all(symbol == '7203' for _, symbol in fast_got)
