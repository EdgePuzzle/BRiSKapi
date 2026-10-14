"""The spool: a recording kept on disk, in Parquet parts, while it is made (briskapi._spool)."""
import json
import os
import subprocess
import sys

import pytest

import briskapi
import briskapi.cli as cli
import briskapi.schema as schema
from briskapi import _parquet, _spool

OPEN_US = 9 * 3600 * 10**6


def session(frames=40, source='brisk_live'):
    """A broker session of two issues: books, trades in every third batch, a history batch, a skipped issue."""
    executed = {0: [0, 0], 1: [0, 0]}   # the trades so far account for each quote's volume and turnover

    def quote(issue, code, i, now):
        return {**dict.fromkeys(schema.QUOTE_KEYS, 0), **{key: [] for key in schema.BOOK_KEYS}, 'issue_id': issue,
                'code': code, 'frame': i + 1, 'max_frame': i + 1, 'source_time_us': now, 'last_price10': 10000 + i,
                'volume': executed[issue][0], 'turnover10': executed[issue][1],
                'bid_levels': [[9990 + i, 100 * (i + 1), 1], [9980, 200, 2]], 'ask_levels': [[10010 + i, 300, 3]]}
    master = [{**dict.fromkeys(schema.MASTER_KEYS, 0), 'issue_id': issue, 'code': code, 'name': name, 'lot_size': 100}
              for issue, code, name in ((0, '7203', 'Toyota'), (1, '6758', 'Sony'))]
    transport = dict(schema.BROKER_TRANSPORTS['sbi'], decoder='/private/decoder.js', ws_host='stream.example',
                     setup_ms=1.5) if schema.broker(source) else {'kind': 'local_cache'}
    batches = [dict(type='bootstrap', seq=0, source=source, trading_date='20260311', source_time_us=OPEN_US,
                    input_transport=transport, source_timestamp_origin='brisk_decoder_unverified', exchange_delay_ms=None,
                    market_issue_count=2, master=list(reversed(master)),
                    quotes=[quote(1, '6758', 0, OPEN_US), quote(0, '7203', 0, OPEN_US)], trades=[]),
               dict(type='history', seq=1, source_time_us=OPEN_US,
                    trades=[dict(issue_id=0, code='7203', frame=1, time_us=OPEN_US - 5, price10=10000, quantity=100, side=1)])]
    for i in range(2, frames + 2):
        now = OPEN_US + i * 100_000
        trades = [dict(issue_id=issue, code=code, frame=i + 1, time_us=now, price10=10000 + i, quantity=100 * k, side=k)
                  for issue, code in ((1, '6758'), (0, '7203')) for k in (1, 2)] if i % 3 == 0 else []
        for t in trades:
            executed[t['issue_id']][0] += t['quantity']
            executed[t['issue_id']][1] += t['price10'] * t['quantity']
        b = dict(type='quotes', seq=i, source_time_us=now, received_unix_ms=1773187200000 + now // 1000, decode_ns=1000 + i,
                 replay_lateness_ms=None, quotes=[quote(1, '6758', i, now), quote(0, '7203', i, now)], trades=trades)
        if i == 7:
            b['quotes'], b['skipped_issues'] = b['quotes'][1:], [1]
        batches.append(b)
    return batches + [dict(type='end', seq=frames + 2, source_time_us=now, frames=frames + 2,
                           quote_updates=2 * frames - 1, replay_wall_ms=4_000.0)]


def lines(batches):
    return [json.dumps(b).encode() + b'\n' for b in batches]


def spooled(tmp_path, batches, part_bytes=1500):
    _spool.PART_BYTES, spool = part_bytes, _spool.Spool(root=tmp_path / 'spool')
    for b, line in zip(batches, lines(batches)):
        spool.add(b, line)
    return spool


@pytest.fixture(autouse=True)
def small_parts(monkeypatch):
    monkeypatch.setattr(_spool, 'PART_BYTES', _spool.PART_BYTES)


@pytest.mark.parametrize('slices, gil', [(16, False), (1, False), (1, True)])
def test_a_spooled_segment_packages_to_the_same_bytes_as_its_batches_at_once(tmp_path, monkeypatch, slices, gil):
    # Parts built in slices on threads (1: a slice from every line), or one at a time with the GIL; the package's
    # summary is the columns' own, which must be the one the archive's checks give (cli.package's).
    monkeypatch.setattr(_parquet, 'SLICE_LINES', slices)
    monkeypatch.setattr(_parquet.sys, '_is_gil_enabled', lambda: gil)
    batches = session()
    (tmp_path / 'events.jsonl').write_bytes(b''.join(lines(batches)))
    whole = cli.package(tmp_path / 'events.jsonl', tmp_path / 'whole')
    spool = spooled(tmp_path, batches)
    spool.close_part(wait=True)
    assert len(spool.parts()) > 5 and spool.ended
    parts = spool.package(tmp_path / 'parts')
    assert parts == whole
    assert all((tmp_path / 'parts' / name).read_bytes() == (tmp_path / 'whole' / name).read_bytes() for name in _parquet.FILES)
    spool.remove()
    assert not spool.path.exists()


def test_parts_hold_no_connection_diagnostics(tmp_path):
    spool = spooled(tmp_path, session())
    spool.close_part(wait=True)
    for folder in spool.parts():
        text = (folder / _parquet.BATCHES).read_bytes() + (spool.path / _parquet.EQUITY).read_bytes()
        assert b'stream.example' not in text and b'/private/decoder.js' not in text
    first = _parquet.pq.read_table(spool.parts()[0] / _parquet.BATCHES).column('input_transport')[0].as_py()
    assert json.loads(first) == schema.BROKER_TRANSPORTS['sbi']


@pytest.mark.parametrize('count', [0, 1, 7, 30, 43])
def test_a_replay_gives_exactly_the_first_lines(tmp_path, count):
    batches = session()
    spool = spooled(tmp_path, batches)
    canonical = [schema.canonical(b) for b in schema.canonical_batches(__import__('io').BytesIO(b''.join(lines(batches))))]
    replay = spool.replay(count)
    got = [json.loads(line) for line in replay]
    assert len(got) == count
    # Written parts come back canonical; lines not yet written come as the decoder wrote them.
    assert [schema.canonical(next(schema.canonical_batches(__import__('io').BytesIO(json.dumps(b).encode()))))
            for b in got] == canonical[:count]


def test_a_replay_keeps_the_spool_until_it_is_read(tmp_path):
    spool = spooled(tmp_path, session())
    spool.close_part(wait=True)
    replay = spool.replay(spool.count)
    spool.remove()
    assert spool.path.exists(), 'still being replayed'
    assert len(list(replay)) == spool.count
    assert not spool.path.exists()
    unread = spooled(tmp_path, session())
    unread.replay(3).close()
    unread.remove()
    assert not unread.path.exists()


def dead_pid():
    return int(subprocess.run([sys.executable, '-c', 'import os; print(os.getpid())'], capture_output=True, text=True).stdout)


def test_leftovers_are_crashed_broker_spools_with_their_sessions_choices_and_a_demos_dropped(tmp_path):
    root, intent = tmp_path / 'spool', {'upload': True, 'output': str(tmp_path / 'days')}
    for source in ('brisk_live', 'historical_mock'):
        _spool.PART_BYTES, spool = 1500, _spool.Spool(root=root, intent=intent)
        for b, line in zip(session(source=source)[:25], lines(session(source=source)[:25])):
            spool.add(b, line)
        spool.close_part(wait=True)
        spool._writer.shutdown()
    for owner in root.glob('segment-*/owner.json'):
        owner.write_text(json.dumps({**json.loads(owner.read_text()), 'pid': dead_pid()}))
    live = _spool.Spool(root=root)   # a running process's own: left alone
    live.add(session()[0], lines(session())[0])
    (live.path / 'owner.json').write_text(json.dumps({'pid': os.getppid(), 'started': 0}))
    found, = _spool.leftovers(root)
    assert found.intent == intent and len(found.parts()) > 3 and repr(found).startswith('Spool(')
    assert sorted(p.name for p in root.iterdir()) == sorted([found.path.name, live.path.name]), 'the demo\'s is gone'
    m = found.package(tmp_path / 'package', repair=True)
    assert m['summary']['batches'] == 26 and m['summary']['source'] == 'brisk_live'
    recording = briskapi.Recording(tmp_path / 'package')
    assert len(briskapi.Market(recording).trades()) == 1 + 4 * 8   # the history's, and batches 3, 6, ... 24


@pytest.mark.parametrize('cut', [1, 2, 10, 25, 41])
def test_a_repaired_end_is_one_the_archive_accepts(tmp_path, cut):
    batches = session()[:cut]
    spool = spooled(tmp_path, batches)
    m = spool.package(tmp_path / 'package', repair=True)
    _parquet.inspect_package(tmp_path / 'package', m)
    end = _parquet._decode_batches(_parquet.pq.read_table(tmp_path / 'package' / _parquet.BATCHES))[-1]
    quotes = [b for b in batches if b['type'] == 'quotes']
    received = [b['received_unix_ms'] for b in quotes]
    assert end == {'type': 'end', 'seq': cut, 'source_time_us': batches[-1]['source_time_us'], 'frames': cut - 1,
                   'quote_updates': sum(len(b['quotes']) for b in quotes),
                   'replay_wall_ms': float(received[-1] - received[0]) if received else 0.0}
    # One that ended needs no repair: the same bytes as without it.
    whole = spooled(tmp_path / 'whole', session())
    columns = _parquet.load_parts(whole.path / _parquet.EQUITY, (whole.close_part(wait=True), whole.parts())[1])
    assert not _parquet.repair_end(columns)


def test_a_damaged_spool_is_left_for_a_look(tmp_path):
    spool = spooled(tmp_path, session()[:10])
    spool.close_part(wait=True)
    (spool.path / 'owner.json').write_text('not json')
    (spool.path / _parquet.EQUITY).unlink()
    with pytest.warns(UserWarning, match='Could not recover'):
        assert _spool.leftovers(tmp_path / 'spool') == []
    assert spool.path.exists(), 'kept for a look, not deleted'
    assert _spool.keep_parts(tmp_path / 'x', [], tmp_path) is None


def test_a_failed_part_write_stops_the_feed(tmp_path, monkeypatch):
    monkeypatch.setattr(_parquet, 'write_part', lambda *a: 1 / 0)
    _spool.PART_BYTES, spool = 1, _spool.Spool(root=tmp_path / 'spool')
    batches = session()
    spool.add(batches[0], lines(batches)[0])
    spool._writer.submit(lambda: None).result()
    with pytest.raises(ZeroDivisionError):
        spool.add(batches[1], lines(batches)[1])
    with pytest.raises(ZeroDivisionError):
        spool.close_part(wait=True)


def test_parts_close_on_time_too(tmp_path, monkeypatch):
    monkeypatch.setattr(_spool, 'PART_SECONDS', 0)
    spool = spooled(tmp_path, session()[:5], part_bytes=10**9)
    spool.close_part(wait=True)
    assert len(spool.parts()) == 5
    assert repr(spool).startswith('Spool(') and _spool.data_dir().name == 'brisk'


def _pieces(spool, tmp_path, final=True, repair=False):
    out = []
    while (shipped := spool.ship(tmp_path / f'piece{len(out)}', final=final, repair=repair)) is not None:
        out.append((tmp_path / f'piece{len(out)}', *shipped))
    return out


def _content(directory):
    """A recording's quotes (issue, frame, values) and trades, whatever its batches are numbered."""
    columns = _parquet.Columns.load(directory)
    quotes = {(q['issue_id'], q['frame']): q for b in columns.iter_batches() if b['type'] != 'bootstrap'
              for q in b.get('quotes', ())}
    trades = [(t['issue_id'], t['frame'], t['quantity']) for b in columns.iter_batches() for t in b.get('trades', ())]
    return quotes, trades


@pytest.mark.parametrize('rows', [1, 6, 15])
def test_a_day_ships_in_pieces_each_a_recording_the_archive_accepts_and_together_the_day(tmp_path, monkeypatch, rows):
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', rows)
    batches = session()
    # Issue 1's trades in batch 9 come without its quote (unreadable there): a piece may end there all the same.
    batches[9]['quotes'], batches[9]['skipped_issues'] = batches[9]['quotes'][1:], [1]
    batches[-1]['quote_updates'] -= 1
    (tmp_path / 'events.jsonl').write_bytes(b''.join(lines(batches)))
    whole = cli.package(tmp_path / 'events.jsonl', tmp_path / 'whole')
    spool = spooled(tmp_path, batches)
    pieces = _pieces(spool, tmp_path)
    assert len(pieces) > 1 and [repaired for *_, repaired in pieces] == [False] * len(pieces)
    for directory, manifest, _ in pieces:
        assert _parquet.inspect_package(directory, manifest) == manifest['summary']   # the archive's own checks
    quotes, trades = {}, []
    for directory, *_ in pieces:
        q, t = _content(directory)
        assert not set(q) & set(quotes), 'no quote twice'
        quotes.update(q)
        trades += t
    day_quotes, day_trades = _content(tmp_path / 'whole')
    assert quotes == day_quotes and sorted(trades) == sorted(day_trades)
    firsts = [m['summary']['first_source_time_us'] for _, m, _ in pieces]
    lasts = [m['summary']['last_source_time_us'] for _, m, _ in pieces]
    assert firsts[0] == whole['summary']['first_source_time_us'] and lasts[-1] == whole['summary']['last_source_time_us']
    assert firsts[1:] == lasts[:-1], 'each piece starts where the one before ended'
    assert spool.ship(tmp_path / 'more', final=True) is None


def _afternoon(batches, at):
    """The session with every batch from `at` on after the lunch break (three and a half hours later)."""
    later = 12_600 * 10**6
    out = []
    for i, b in enumerate(json.loads(json.dumps(batches))):
        if i >= at:
            b['source_time_us'] += later
            if 'received_unix_ms' in b:
                b['received_unix_ms'] += later // 1000
            for q in b.get('quotes', ()):
                q['source_time_us'] += later
            for t in b.get('trades', ()):
                t['time_us'] += later
        out.append(b)
    return out


def test_a_day_ships_its_morning_once_a_part_closes_after_noon_and_its_afternoon_at_its_end(tmp_path, monkeypatch):
    monkeypatch.setattr(_spool, 'LUNCH_US', OPEN_US + 3 * 3600 * 10**6)   # the session's clocks start at OPEN_US
    batches = _afternoon(session(), 20)
    spool = spooled(tmp_path, batches[:24])   # the day runs on past noon: the morning ships
    spool.close_part(wait=True)
    morning = spool.ship(tmp_path / 'morning')
    assert morning and morning[0]['summary']['last_source_time_us'] >= _spool.LUNCH_US > morning[0]['summary']['first_source_time_us']
    assert spool.ship(tmp_path / 'again') is None, 'the afternoon waits for the day\'s end'
    for b, line in zip(batches[24:], lines(batches[24:])):
        spool.add(b, line)
    afternoon = spool.ship(tmp_path / 'afternoon', final=True)
    assert afternoon and afternoon[0]['summary']['first_source_time_us'] == morning[0]['summary']['last_source_time_us']
    for directory, (manifest, _) in ((tmp_path / 'morning', morning), (tmp_path / 'afternoon', afternoon)):
        assert _parquet.inspect_package(directory, manifest) == manifest['summary']
    assert spool.ship(tmp_path / 'more', final=True) is None


def test_a_piece_written_a_range_of_issues_at_a_time_is_the_same_as_written_at_once(tmp_path, monkeypatch):
    monkeypatch.setattr(_parquet, 'PIECE_ROWS', 3)   # many ranges
    monkeypatch.setattr(_parquet, 'PART_ISSUES', 1)
    spool = spooled(tmp_path, session())
    spool.close_part(wait=True)
    parts, equity = spool.parts(), spool.path / _parquet.EQUITY
    bootstrap = _parquet.bootstrap_row(parts[0])
    midnight = _parquet.midnight_ns(bootstrap['trading_date'])
    k = len(parts) // 2
    before = _parquet.load_parts(equity, parts[:k], midnight)
    cut = _spool._Cut(_parquet.latest(before), before.batches[-1]['source_time_us'])
    expected = _parquet.after(_parquet.load_parts(equity, parts[k:], midnight), cut, bootstrap)
    _parquet.recount_end(expected)
    expected.write(tmp_path / 'at-once')
    summary, latest, clock, repaired = _parquet.write_piece(equity, parts[k:], tmp_path / 'by-range', midnight, cut, bootstrap)
    for name in _parquet.FILES:
        assert (tmp_path / 'by-range' / name).read_bytes() == (tmp_path / 'at-once' / name).read_bytes(), name
    assert summary == expected.summary() and not repaired and clock == expected.batches[-1]['source_time_us']
    assert len(latest) == len(_parquet.latest(expected))


def test_a_crashed_spool_ships_what_is_left_from_where_it_was(tmp_path, monkeypatch):
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', 8)
    batches = session()[:30]   # cut short: no end
    spool = spooled(tmp_path, batches)
    spool.close_part(wait=True)
    first = spool.ship(tmp_path / 'first')
    assert first is not None and json.loads((spool.path / 'shipped.json').read_text())['parts'] == len(spool.last_parts)
    spool._writer.shutdown()
    again = _spool.Spool.reopen(spool.path)
    rest = _pieces(again, tmp_path, repair=True)
    assert rest and rest[-1][2] is True, 'the last piece repaired with the end its host never wrote'
    for directory, manifest, _ in rest:
        assert _parquet.inspect_package(directory, manifest) == manifest['summary']
    shipped = {k for d in [tmp_path / 'first', *(d for d, *_ in rest)] for k in _content(d)[0]}
    assert len(shipped) == sum(len(b.get('quotes', ())) for b in batches[1:])
