"""brisk-parquet (rust/brisk_parquet) against Python's rules for a broker's recording (tests/reference_verifier.py)."""
import copy
import decimal
import json
import random
import shutil

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import briskapi.schema as schema
from briskapi import _native, _parquet

import reference_verifier
from test_broker_verify import broker, spoiled  # noqa: F401  (the fixture and its helper)


def outcome(function, *arguments):
    """The summary a verifier gives, or the message of its refusal (what a library says of a value it cannot read, in
    the words brisk-parquet uses)."""
    try:
        return function(*arguments)
    except ValueError as error:
        text = str(error)
        if 'Rescaling Decimal' in text:
            return 'Invalid decimal value'
        if 'line 1 column' in text or 'Expecting' in text:
            return 'Invalid broker transport fields'
        return text


def test_the_rust_verifier_gives_the_summary_python_does(tmp_path, broker):  # noqa: F811
    m, _ = broker
    package = tmp_path / 'package'
    assert reference_verifier.verify_broker(package, m) == _parquet.verify_broker(package, m) == m['summary']
    assert json.loads(_native.run(['verify', package])) == m['summary']


def test_a_missing_program_or_table_is_told_plainly(tmp_path, broker, monkeypatch):  # noqa: F811
    m, _ = broker
    shutil.rmtree(tmp_path / 'package' / 'data' / 'custom')
    with pytest.raises(ValueError, match='Missing data/custom/BriskBatch.parquet'):
        _parquet.verify_broker(tmp_path / 'package', m)
    monkeypatch.setenv('BRISK_PARQUET', str(tmp_path / 'nowhere'))
    monkeypatch.setattr(_native, '_HERE', tmp_path)
    monkeypatch.setattr(_native.shutil, 'which', lambda name: None)
    with pytest.raises(RuntimeError, match='not installed'):
        _native.binary()


def test_progress_is_told_as_each_table_is_done(tmp_path, broker):  # noqa: F811
    m, _ = broker
    seen = []
    _parquet.verify_broker(tmp_path / 'package', m, seen.append)
    assert seen == ['verify: batches', 'verify: depths', 'verify: trades']


def rewrite(directory, name, table):
    path = directory / name
    pq.write_table(table, path, row_group_size=_parquet.ROW_GROUP, **_parquet.options(name))


def mutate(rng, table):
    """The table with one cell changed (or a list's level moved), and what was done."""
    names = table.schema.names
    c = rng.randrange(len(names))
    name, column = names[c], table.column(c).combine_chunks()
    row = rng.randrange(len(column))
    kind = column.type
    nullable = table.schema.field(c).nullable

    def pick(choices):
        return rng.choice([v for v in choices if v is not None or nullable])
    if pa.types.is_timestamp(kind):
        values = column.cast(pa.int64()).to_pylist()
        was = values[row]
        values[row] = pick([None, was + 1000, was - 1000, was + 1, 0, was + 10**15]) if was is not None else 1000
        mutated = pa.array(values, pa.int64()).cast(kind)
    elif pa.types.is_integer(kind) or pa.types.is_floating(kind):
        values = column.to_pylist()
        was = values[row]
        choices = [None, 0, 2**53, 2**53 - 1, 1] if was is None else [None, 0, was + 1, was + 1000, max(was - 1, 0), 2**53, 2**32]
        if pa.types.is_floating(kind):
            choices = [None, 0.0, -0.0, 1.0005, 1.5, 86400001.0, float('nan'), 12.345, 1e300]
        value = pick(choices)
        values[row] = min(value, 255) if kind == pa.uint8() and value is not None else value
        mutated = pa.array(values, kind)
    elif pa.types.is_decimal(kind):
        values = column.to_pylist()
        step = rng.choice([decimal.Decimal('0.1'), decimal.Decimal('-0.1'), decimal.Decimal('0.0000000000000001'),
                           decimal.Decimal('1'), decimal.Decimal('9007199254740993')])
        values[row] = pick([None, decimal.Decimal(0), (values[row] or decimal.Decimal(0)) + step])
        mutated = pa.array(values, kind)
    elif pa.types.is_string(kind):
        values = column.to_pylist()
        was = values[row]
        values[row] = pick([None, '', (was or '') + 'x', 'X' + (was or ''), 'BriskBatch', '7203.XTKS', '0-0', '1-1'])
        mutated = pa.array(values, kind)
    elif pa.types.is_dictionary(kind):
        values = column.cast(pa.string()).to_pylist()
        values[row] = rng.choice(['BUY', 'SELL', 'NO_AGGRESSOR'])
        mutated = pa.array(values, pa.string()).dictionary_encode().cast(kind)
    elif pa.types.is_list(kind):
        values = column.to_pylist()
        item = values[row]
        if item is None:
            values[row] = pick([None, []])
        else:
            move = rng.choice(['drop', 'swap', 'duplicate', 'empty', 'many', 'order_id', 'bump'])
            if move == 'drop' and item:
                item.pop()
            elif move == 'swap' and len(item) > 1:
                item[0], item[1] = item[1], item[0]
            elif move == 'duplicate' and item:
                item.append(copy.deepcopy(item[-1]))
            elif move == 'empty':
                values[row] = []
            elif move == 'many' and item and isinstance(item[0], dict):
                item.extend(copy.deepcopy(item[0]) for _ in range(11))
            elif move == 'order_id' and item and isinstance(item[0], dict):
                item[0]['order_id'] = 1
            elif move == 'bump' and item:
                if isinstance(item[0], dict):
                    item[0]['price'] += decimal.Decimal('0.1')
                else:
                    item[0] = item[0] + 1
        mutated = pa.array(values, kind)
    else:
        raise AssertionError(kind)
    return table.set_column(c, table.schema.field(c), mutated), f'{name}[{row}]'


@pytest.mark.parametrize('seed', range(8))
def test_a_cell_changed_anywhere_is_refused_by_both_or_neither(tmp_path, broker, seed):  # noqa: F811
    """Mutate one cell of one table, over and over: the Rust verifier and Python's rules must agree on every one (the
    summary, or the same refusal)."""
    m, _ = broker
    rng = random.Random(seed)
    disagreements, refused = [], 0
    for trial in range(40):
        target = tmp_path / f'case{trial}'
        shutil.copytree(tmp_path / 'package', target)
        name = rng.choice(_parquet.FILES)
        table, what = mutate(rng, pq.read_table(target / name))
        rewrite(target, name, table)
        python = outcome(reference_verifier.verify_broker, target, m)
        rust = outcome(_parquet.verify_broker, target, m)
        refused += isinstance(python, str)
        if python != rust:
            disagreements.append((name, what, python, rust))
        shutil.rmtree(target)
    assert not disagreements, disagreements
    assert refused > 10   # the mutations are not all harmless


def test_both_agree_on_what_the_summary_is_for_many_row_groups(tmp_path, broker, monkeypatch):  # noqa: F811
    m, columns = broker
    monkeypatch.setattr(_parquet, 'ROW_GROUP', 3)
    target = spoiled(tmp_path, columns, 'groups')
    assert reference_verifier.verify_broker(target) == _parquet.verify_broker(target) == m['summary']
    order = np.lexsort((columns.q['seq'], columns.q['issue_id']))[::-1]
    columns.order = None
    other = spoiled(tmp_path, columns.take(order), 'reversed')
    assert outcome(reference_verifier.verify_broker, other) == outcome(_parquet.verify_broker, other)


def test_a_package_must_be_in_the_format_its_manifest_names(tmp_path, broker):  # noqa: F811
    m, _ = broker
    with pytest.raises(ValueError, match='Unexpected schema'):
        _parquet.verify_broker(tmp_path / 'package', dict(m, schema=schema.LEGACY_SCHEMA))


def test_the_expected_schemas_are_python_s(tmp_path):
    """brisk-parquet writes an empty table of each kind: pyarrow reads exactly the schema and metadata Python defines."""
    _native.run(['schemas', tmp_path])
    for name, expected in _parquet.SCHEMAS.items():
        found = pq.ParquetFile(tmp_path / name)
        assert found.metadata.schema.to_arrow_schema().equals(expected, check_metadata=True), name
        assert found.metadata.num_rows == 0
        assert set(found.metadata.metadata) == {b'ARROW:schema', *expected.metadata}


# Packaging: parts of a spooled segment, as one recording

from test_spool import same_tables, session, spooled  # noqa: E402  (the segment's fixtures)
import reference_package  # noqa: E402


def two_ways(tmp_path, spool, parts, k=None, end=None, range_rows=None):
    """Package `parts` with Python's write_piece and with brisk-parquet, the second after a piece of `parts[:k]` if `k`."""
    equity = spool.path / _parquet.EQUITY
    bootstrap = _parquet.bootstrap_row(spool.parts()[0])
    midnight = _parquet.midnight_ns(bootstrap['trading_date'])
    cut = state = clock = None
    mine, theirs = tmp_path / 'rust', tmp_path / 'python'
    if k:
        first = _native.package(spool.path, parts[:k], tmp_path / 'first', midnight, end='cut', state_out=tmp_path / 'state.parquet',
                                range_rows=range_rows)
        _, latest, clock, _ = reference_package.write_piece(equity, parts[:k], tmp_path / 'first-python', midnight, None, bootstrap, 'cut')
        same_tables(tmp_path / 'first', tmp_path / 'first-python')
        cut, state = (tmp_path / 'state.parquet', first['clock']), reference_package.Cut(latest, clock)
        assert first['clock'] == clock
        parts = parts[k:]
    result = _native.package(spool.path, parts, mine, midnight, day_start=spool.parts()[0], cut=cut, end=end or 'parts',
                             state_out=tmp_path / 'state2.parquet', range_rows=range_rows)
    summary, latest, clock, repaired = reference_package.write_piece(equity, parts, theirs, midnight, state, bootstrap, end)
    same_tables(mine, theirs)
    assert result['summary'] == summary and result['clock'] == clock and result['repaired'] == repaired
    after = pq.read_table(tmp_path / 'state2.parquet')
    assert after.column('issue_id').to_pylist() == latest.q['issue_id'].tolist()
    assert after.column('sequence').to_pylist() == latest.q['frame'].tolist()
    return result


@pytest.fixture
def segment(tmp_path):
    spool = spooled(tmp_path, session())
    spool.close_part(wait=True)
    assert len(spool.parts()) > 5
    return spool


def test_a_whole_segment_is_packaged_as_python_does(tmp_path, segment):
    two_ways(tmp_path, segment, segment.parts())
    result = json.loads(_native.run(['verify', tmp_path / 'rust']))
    assert result == json.loads((tmp_path / 'rust' / 'summary.json').read_text()) if (tmp_path / 'rust' / 'summary.json').exists() else True


@pytest.mark.parametrize('k', [1, 2, 3, 4])
def test_a_piece_after_a_cut_is_packaged_as_python_does(tmp_path, segment, k):
    result = two_ways(tmp_path, segment, segment.parts(), k=k)
    assert json.loads(_native.run(['verify', tmp_path / 'rust'])) == result['summary']
    assert json.loads(_native.run(['verify', tmp_path / 'first'])) is not None


def test_a_piece_that_stops_early_gets_an_end(tmp_path, segment):
    result = two_ways(tmp_path, segment, segment.parts()[:3], end='cut')
    assert json.loads(_native.run(['verify', tmp_path / 'rust'])) == result['summary'] and not result['repaired']


def test_a_segment_cut_short_is_repaired(tmp_path):
    spool = spooled(tmp_path, session()[:30])
    spool.close_part(wait=True)
    result = two_ways(tmp_path, spool, spool.parts(), end='repair')
    assert result['repaired'] and json.loads(_native.run(['verify', tmp_path / 'rust'])) == result['summary']


def test_a_piece_written_a_range_of_issues_at_a_time_is_the_same_as_written_at_once(tmp_path, segment, monkeypatch):
    monkeypatch.setattr(reference_package, 'PIECE_ROWS', 3)   # many ranges, each a few issues' rows
    monkeypatch.setattr(reference_package, 'PART_ISSUES', 1)
    monkeypatch.setattr(reference_package, 'DECODE_ROWS', 4)
    result = two_ways(tmp_path, segment, segment.parts(), k=len(segment.parts()) // 2, range_rows=3)
    assert json.loads(_native.run(['verify', tmp_path / 'rust'])) == result['summary']
