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


def session(frames=40, source='sbi_live'):
    """A broker session of two issues: books, trades in every third batch, a history batch, a skipped issue."""
    executed = {0: [0, 0], 1: [0, 0]}   # the trades so far account for each quote's volume and turnover

    def quote(issue, code, i, now):
        return {**dict.fromkeys(schema.QUOTE_KEYS, 0), **{key: [] for key in schema.BOOK_KEYS}, 'issue_id': issue,
                'code': code, 'frame': i + 1, 'max_frame': i + 1, 'source_time_us': now, 'last_price10': 10000 + i,
                'volume': executed[issue][0], 'turnover10': executed[issue][1],
                'bid_levels': [[9990 + i, 100 * (i + 1), 1], [9980, 200, 2]], 'ask_levels': [[10010 + i, 300, 3]]}
    master = [{**dict.fromkeys(schema.MASTER_KEYS, 0), 'issue_id': issue, 'code': code, 'name': name, 'lot_size': 100}
              for issue, code, name in ((0, '7203', 'Toyota'), (1, '6758', 'Sony'))]
    transport = dict(schema.BROKER_TRANSPORT[source], decoder='/private/decoder.js', ws_host='stream.example',
                     setup_ms=1.5) if source in schema.BROKER_TRANSPORT else {'kind': 'local_cache'}
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


def test_a_spooled_segment_packages_to_the_same_bytes_as_its_batches_at_once(tmp_path):
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
    assert json.loads(first) == schema.BROKER_TRANSPORT['sbi_live']


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


def test_a_crashed_feeds_spool_is_kept_as_a_recording_and_a_demos_dropped(tmp_path):
    root, out = tmp_path / 'spool', tmp_path / 'kept'
    for source in ('sbi_live', 'historical_mock'):
        spool = spooled(tmp_path, session(source=source)[:25])
        spool.close_part(wait=True)
        spool._writer.shutdown()
    dead = subprocess.run([sys.executable, '-c', 'import os; print(os.getpid())'], capture_output=True, text=True)
    for owner in root.glob('segment-*/owner.json'):
        owner.write_text(json.dumps({'pid': int(dead.stdout), 'started': 0}))
    live = _spool.Spool(root=root)   # this process's own: left alone
    live.add(session()[0], lines(session())[0])
    (live.path / 'owner.json').write_text(json.dumps({'pid': os.getppid(), 'started': 0}))
    kept = _spool.recover(out, root=root)
    assert [k.name for k in kept] == ['sbi_live_20260311_090000-090002'] and not (kept[0] / 'manifest.json').exists()
    recording = briskapi.Recording(kept[0])
    assert len(briskapi.Market(recording).trades()) == 1 + 4 * 8   # the history's, and batches 3, 6, ... 24
    assert sorted(p.name for p in root.iterdir()) == [live.path.name]


def test_recover_keeps_in_kept_dir_without_output_and_skips_damage(tmp_path):
    spool = spooled(tmp_path, session()[:10])
    spool.close_part(wait=True)
    (spool.path / 'owner.json').write_text('not json')
    (spool.path / _parquet.EQUITY).unlink()
    with pytest.warns(UserWarning, match='Could not recover'):
        assert _spool.recover(root=tmp_path / 'spool') == []
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
