"""The archive's checks of a broker's recording, a row group at a time (briskapi._parquet.verify_broker)."""
import json
from pathlib import Path

import numpy as np
import pytest

import briskapi.cli as cli
import briskapi.schema as schema
from briskapi import _parquet

import reference_verifier
from test_spool import lines, session


@pytest.fixture
def broker(tmp_path):
    """A broker's recording, packaged, and its tables in memory to spoil."""
    (tmp_path / 'events.jsonl').write_bytes(b''.join(lines(session())))
    m = cli.package(tmp_path / 'events.jsonl', tmp_path / 'package')
    return m, _parquet.Columns.load(tmp_path / 'package')


def spoiled(tmp_path, columns, name='spoiled'):
    """Write columns as a recording, canonically encoded, so that only what they hold is wrong."""
    columns.order = None
    columns._trade_ids = None
    columns.write(tmp_path / name)
    return tmp_path / name


def test_a_broker_recording_passes_with_the_summary_the_archive_computes(tmp_path, broker):
    m, columns = broker
    assert _parquet.verify_broker(tmp_path / 'package', m) == m['summary']
    assert _parquet.verify_package(tmp_path / 'package', m) == m['summary']
    # The full checks agree, and so does the summary the columns give.
    assert schema.validate_batches(columns.iter_batches()) == m['summary'] == columns.summary()


def test_a_broker_package_may_be_far_larger_than_a_replay(broker):
    m, _ = broker
    big = dict(m, bytes=1024**3)
    assert schema.validate_manifest(big)
    with pytest.raises(ValueError, match='Invalid unsigned integer'):
        schema.validate_manifest(dict(m, bytes=schema.MAX_BROKER_COMPRESSED + 1))
    demo = dict(big, summary=dict(m['summary'], source='synthetic_test'))
    with pytest.raises(ValueError):
        schema.validate_manifest(demo)


def _quote_rows(columns, issue):
    return np.flatnonzero(columns.q['issue_id'] == issue)


@pytest.mark.parametrize('spoil, message', [
    (lambda c: c.q['issue_id'].__setitem__(_quote_rows(c, 1), 7), 'Unknown issue'),
    (lambda c: c.code.__setitem__(_quote_rows(c, 1), '7203'), 'identity mismatch'),
    (lambda c: c.q['source_time_us'].__setitem__(-1, c.q['source_time_us'][-1] + 10**9), 'after their batch clock'),
    (lambda c: c.q['seq'].__setitem__(_quote_rows(c, 0)[-1], len(c.batches) - 1), 'outside their batches'),
    (lambda c: c.q['frame'].__setitem__(_quote_rows(c, 0)[5], 0), 'regression'),
    (lambda c: c.books['bid_levels'][1].__setitem__((slice(0, 2), 0), [9000, 9990]), 'Book prices out of order'),
    (lambda c: c.t['price10'].__setitem__(0, 0), 'Invalid trade'),
    (lambda c: c.t['seq'].__setitem__(-1, len(c.batches) - 1), 'outside their batches'),
])
def test_quotes_and_trades_that_are_not_well_formed_are_refused(tmp_path, broker, spoil, message):
    _, columns = broker
    spoil(columns)
    with pytest.raises(ValueError, match=message):
        _parquet.verify_broker(spoiled(tmp_path, columns))


@pytest.mark.parametrize('spoil, message', [
    (lambda b: b[3].__setitem__('seq', 5), 'Sequence gap'),
    (lambda b: b[4].__setitem__('source_time_us', b[3]['source_time_us'] - 1), 'Market clock regression'),
    (lambda b: b[0].__setitem__('input_transport', {**b[0]['input_transport'], 'ws_host': 'stream.example'}), 'transport'),
    (lambda b: b[-1].__setitem__('quote_updates', 1), 'End summary mismatch'),
    (lambda b: b[5].__setitem__('received_unix_ms', 1), 'Implausible local receipt clock'),
    (lambda b: b[9].__setitem__('received_unix_ms', b[2]['received_unix_ms'] + schema.MAX_SESSION_MS + 1), 'receipt clock span'),
    (lambda b: b[-1].__setitem__('frames', sum(x['type'] == 'quotes' for x in b) - 1), 'End summary mismatch'),
    (lambda b: b[5].__setitem__('replay_lateness_ms', 1.0), 'not demo replays'),
    (lambda b: b.pop(), 'no clean end'),
    (lambda b: b[7].__setitem__(schema.SKIPPED, [0]), 'Invalid skipped issues'),
    (lambda b: b[0].__setitem__(schema.SKIPPED, [1]), 'Invalid skipped issues'),
])
def test_batches_that_break_the_archives_rules_are_refused(tmp_path, broker, spoil, message):
    _, columns = broker
    spoil(columns.batches)
    with pytest.raises(ValueError, match=message):
        _parquet.verify_broker(spoiled(tmp_path, columns))


def three_verdicts(tmp_path, columns, name):
    """What Python's full rules (schema), brisk-parquet and the reference verifier say of the same recording: its summary, or the
    refusal's message."""
    written = None

    def verdict(check):
        nonlocal written
        try:
            return check()
        except ValueError as error:
            return str(error)
    python = verdict(lambda: schema.validate_batches(columns.iter_batches()))
    written = spoiled(tmp_path, columns, name)
    return python, verdict(lambda: _parquet.verify_broker(written)), verdict(lambda: reference_verifier.verify_broker(written))


def test_history_batches_are_not_frames_so_an_end_may_count_fewer_frames_than_batches(tmp_path, broker):
    # A host that joins mid-session restores the day's trades as history batches, which take a sequence number each but are no
    # frames; its end counts the frames it received (a quote batch each, and any before the bootstrap).
    m, columns = broker
    quote_batches = sum(b['type'] == 'quotes' for b in columns.batches)
    assert sum(b['type'] == 'history' for b in columns.batches) >= 1
    columns.batches[-1]['frames'] = quote_batches
    assert quote_batches < columns.batches[-1]['seq'] - 1, 'fewer frames than there are batches before the end'
    python, rust, reference = three_verdicts(tmp_path, columns, 'host')
    assert isinstance(python, dict) and python['batches'] == rust['batches'] == reference['batches']
    assert python == rust == reference
    # Fewer than one for each quote batch cannot be.
    columns.batches[-1]['frames'] = quote_batches - 1
    assert three_verdicts(tmp_path, columns, 'short') == ('End summary mismatch',) * 3


def test_the_receipt_clock_is_timing_not_order_so_it_may_go_back_and_is_bound_only_by_its_span(tmp_path, broker):
    # A computer adjusting its clock sets received_unix_ms back for a few batches (a tester's, by 47 to 90 ms, in two feeds at
    # once). It is a local measurement for timing (ts_recv, the timing reports): no rule orders it, however far it goes back.
    m, columns = broker
    quotes = [b for b in columns.batches if b['type'] == 'quotes']
    first = quotes[0]['received_unix_ms']
    for k, b in enumerate(quotes[9:19]):
        b['received_unix_ms'] -= 90 - k * 5
    python, rust, reference = three_verdicts(tmp_path, columns, 'adjusted')
    assert isinstance(python, dict) and python == rust == reference
    quotes[9]['received_unix_ms'] -= 5_000            # seconds back
    quotes[12]['received_unix_ms'] = first - 3_600_000   # an hour before the first
    python, rust, reference = three_verdicts(tmp_path, columns, 'back')
    assert isinstance(python, dict) and python == rust == reference
    # What it is bound by: a day either side of the first quote batch's (and the range of a plausible date, tested above).
    quotes[12]['received_unix_ms'] = first - schema.MAX_SESSION_MS
    assert isinstance(three_verdicts(tmp_path, columns, 'edge')[1], dict)
    quotes[12]['received_unix_ms'] = first - schema.MAX_SESSION_MS - 1
    assert three_verdicts(tmp_path, columns, 'before') == ('Local receipt clock span',) * 3
    quotes[12]['received_unix_ms'] = first + schema.MAX_SESSION_MS + 1
    assert three_verdicts(tmp_path, columns, 'after') == ('Local receipt clock span',) * 3


def test_a_bootstrap_without_every_issue_is_refused(tmp_path, broker):
    _, columns = broker
    keep = np.flatnonzero(~((columns.q['seq'] == 0) & (columns.q['issue_id'] == 1)))
    with pytest.raises(ValueError, match='Incomplete bootstrap'):
        _parquet.verify_broker(spoiled(tmp_path, columns.take(keep)))


def test_tables_that_are_not_their_canonical_encoding_or_carry_more_are_refused(tmp_path, broker):
    m, columns = broker
    import pyarrow.parquet as pq
    source = tmp_path / 'package'
    # Another compression than the canonical one: structure.
    target = spoiled(tmp_path, columns, 'recompressed')
    pq.write_table(pq.read_table(source / _parquet.DEPTHS), target / _parquet.DEPTHS, compression='snappy')
    with pytest.raises(ValueError, match='Unexpected (schema|compression)|Non-canonical'):
        _parquet.verify_broker(target)
    # A derived column that disagrees with what it derives from: canonical encoding.
    target = spoiled(tmp_path, columns, 'derived')
    table = pq.read_table(target / _parquet.DEPTHS)
    flags = table.column('flags').to_numpy().copy()
    flags[0] ^= 1
    import pyarrow as pa
    table = table.set_column(table.schema.get_field_index('flags'), table.schema.field('flags'), pa.array(flags, pa.uint8()))
    pq.write_table(table, target / _parquet.DEPTHS, row_group_size=_parquet.ROW_GROUP, **_parquet.WRITE)
    with pytest.raises(ValueError, match='Non-canonical'):
        _parquet.verify_broker(target)
    # A summary that is not the recording's.
    with pytest.raises(ValueError, match='Summary mismatch'):
        _parquet.verify_broker(source, dict(m, summary=dict(m['summary'], quote_updates=m['summary']['quote_updates'] + 1)))


def test_quotes_out_of_order_across_row_groups_are_refused(tmp_path, broker, monkeypatch):
    m, columns = broker
    monkeypatch.setattr(_parquet, 'ROW_GROUP', 4)   # many row groups, each checked on its own
    assert _parquet.verify_broker(spoiled(tmp_path, columns, 'small'))['quote_updates'] == len(columns)
    order = np.lexsort((columns.q['seq'], columns.q['issue_id']))[::-1]
    with pytest.raises(ValueError, match='sorted'):
        _parquet.verify_broker(spoiled(tmp_path, columns.take(order), 'reversed'))
    assert json.loads((tmp_path / 'package' / 'manifest.json').read_text()) == m


@pytest.mark.parametrize('spoil, message', [
    (lambda b: b[4].__setitem__('source_time_us', b[3]['source_time_us'] - 1), 'Market clock regression'),
    (lambda b: b[8].__setitem__('seq', 7), 'Sequence gap'),
    (lambda b: b[4].__setitem__('received_unix_ms', 1), 'Implausible local receipt clock'),
    (lambda b: b[9].__setitem__('received_unix_ms', b[2]['received_unix_ms'] + schema.MAX_SESSION_MS + 1), 'receipt clock span'),
    (lambda b: b[9].__setitem__('replay_lateness_ms', 1.0), 'not demo replays'),
])
def test_the_batch_rules_hold_across_row_groups(tmp_path, broker, monkeypatch, spoil, message):
    # Batches are checked a row group at a time, carrying what each rule needs from one to the next.
    monkeypatch.setattr(_parquet, 'ROW_GROUP', 4)
    m, columns = broker
    good = spoiled(tmp_path, columns, 'good')
    assert _parquet.verify_broker(good)['batches'] == len(columns.batches), 'and a clean recording passes in many groups'
    assert _parquet.pq.ParquetFile(good / _parquet.BATCHES).num_row_groups > 5
    spoil(columns.batches)
    with pytest.raises(ValueError, match=message):
        _parquet.verify_broker(spoiled(tmp_path, columns, 'spoiled-groups'))


def test_a_recording_with_a_million_batches_is_verified_without_holding_them_all(tmp_path):
    """The batches as dicts take 1.6 GB a million: the archive's function has 3 GB. Rows are a row group at a time."""
    import subprocess, sys
    resource = pytest.importorskip('resource')   # not on Windows, where the function does not run either
    write = (f"import sys; sys.path.insert(0, 'tests')\nimport io, json\nfrom briskapi import _parquet, schema\n"
             f"from test_spool import session\nbase = session(frames=8)\nboot, n = base[0], 1_000_000\n"
             f"issue = boot['master'][0]['issue_id']\n"
             f"boot = dict(boot, master=[boot['master'][0]], market_issue_count=1,\n"
             f"    quotes=[q for q in boot['quotes'] if q['issue_id'] == issue][:1])\n"
             f"boot = next(schema.canonical_batches(io.BytesIO(json.dumps(boot).encode() + b'\\n')))\n"
             f"O = 9 * 3600 * 10**6\nmid = [dict(type='quotes', seq=i, source_time_us=O + i * 20, received_unix_ms=1773187200000 + i // 50,\n"
             f"    decode_ns=1000 + i % 500, replay_lateness_ms=None, quotes=[], trades=[]) for i in range(1, n)]\n"
             f"_parquet.Columns.from_batches([boot, *mid, dict(type='end', seq=n, source_time_us=O + n * 20, frames=n,\n"
             f"    quote_updates=0, replay_wall_ms=1.0)]).write({str(tmp_path / 'big')!r})")
    subprocess.run([sys.executable, '-c', write], check=True, cwd=Path(__file__).parent.parent)
    verify = (f"import resource\nfrom briskapi import _parquet\n"
              f"s = _parquet.verify_broker({str(tmp_path / 'big')!r})\n"
              f"print(s['batches'], resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 2**20)")
    out = subprocess.run([sys.executable, '-c', verify], check=True, capture_output=True, text=True,
                         cwd=Path(__file__).parent.parent).stdout.split()
    assert int(out[0]) == 1_000_000 + 1 and int(out[1]) < 1200, f'{out[1]} MB for a million batches (3000 before they were streamed)'
