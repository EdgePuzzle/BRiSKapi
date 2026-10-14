"""The table format: v3 keeps one clock in the tables of updates and delta-encodes rising columns; v2 recordings
(with ts_init, a copy of ts_event) read, check and upload as before."""
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import archive_service as service
import briskapi
import briskapi.cli as cli
import briskapi.schema as schema
from briskapi import _parquet

from test_archive import DB, S3, batches, isolated, raw, stored  # noqa: F401 - isolated: the archive's test configuration
from test_spool import lines, session


def legacy(directory, target, ts_init=None):
    """A recording's tables as a client before v3 wrote them, and their manifest: ts_init back in the tables of
    updates (or, given, other values), every column in a dictionary."""
    m = json.loads((directory / 'manifest.json').read_text())
    for name in _parquet.FILES:
        table = _parquet.as_format(pq.read_table(directory / name), name, schema.LEGACY_SCHEMA)
        if ts_init is not None and name == _parquet.DEPTHS:
            field = table.schema.get_field_index('ts_init')
            table = table.set_column(field, table.schema.field(field), pa.array([ts_init] * len(table), _parquet.TIME))
        (target / name).parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, target / name, row_group_size=_parquet.ROW_GROUP, **_parquet.WRITE)
    sha256, size = _parquet.pack_to(target, target.parent / f'{target.name}.zip')
    m = dict(m, schema=schema.LEGACY_SCHEMA, sha256=sha256, bytes=size)
    (target / 'manifest.json').write_text(json.dumps(m))
    return m


@pytest.fixture(params=['broker', 'synthetic'])
def package(request, tmp_path):
    events = tmp_path / 'events.jsonl'
    events.write_bytes(b''.join(lines(session())) if request.param == 'broker' else raw(batches()))
    return tmp_path / 'package', cli.package(events, tmp_path / 'package')


def test_the_tables_of_updates_keep_one_clock_and_delta_encode_what_rises(package):
    directory, m = package
    assert m['schema'] == schema.SCHEMA == 'brisk-nautilus-parquet-v3'
    for name in (_parquet.DEPTHS, _parquet.TRADES, _parquet.BATCHES):
        meta = pq.ParquetFile(directory / name).metadata
        assert 'ts_init' not in meta.schema.names
        column = meta.schema.names.index('ts_event')
        if meta.num_row_groups:
            assert 'DELTA_BINARY_PACKED' in meta.row_group(0).column(column).encodings
    assert 'ts_init' in pq.read_schema(directory / _parquet.EQUITY).names   # v2's Equity, one row a security


def test_a_v2_recording_reads_checks_and_exports_as_before(package, tmp_path):
    directory, m = package
    old = legacy(directory, tmp_path / 'v2')
    assert _parquet.check(tmp_path / 'v2') == schema.LEGACY_SCHEMA and _parquet.check(directory) == schema.SCHEMA
    assert _parquet.inspect_package(tmp_path / 'v2', old) == m['summary']
    assert list(briskapi.Recording(tmp_path / 'v2').batches()) == list(briskapi.Recording(directory).batches())
    assert old['bytes'] > m['bytes'], 'v3 is smaller'
    # The catalog has ts_init either way: NautilusTrader's layout needs it.
    for source in (directory, tmp_path / 'v2'):
        files = _parquet.export_nautilus(source, tmp_path / f'catalog-{source.name}')
        depths = [f for f in files if 'order_book_depths' in str(f)]
        assert depths and all(pq.read_schema(f).names == _parquet.V2_DEPTH for f in depths)
        table = pq.read_table(depths[0])
        assert table.column('ts_init').equals(table.column('ts_event'))
    # A manifest must name the format its tables are in.
    with pytest.raises(ValueError, match='Unexpected schema'):
        _parquet.verify_package(tmp_path / 'v2', dict(old, schema=schema.SCHEMA))


def test_a_v2_ts_init_that_is_not_the_source_clock_is_refused(package, tmp_path):
    directory, _ = package
    old = legacy(directory, tmp_path / 'v2', ts_init=10**18)
    with pytest.raises(ValueError, match='Non-canonical'):
        _parquet.verify_package(tmp_path / 'v2', old)


def test_the_archive_takes_both_formats_and_keeps_a_replays_market_data_once_for_each(tmp_path):
    s3, db = S3(), DB()
    events = tmp_path / 'events.jsonl'
    events.write_bytes(raw(batches()))
    m = cli.package(events, tmp_path / 'v3')
    old = legacy(tmp_path / 'v3', tmp_path / 'v2')
    for directory, manifest in ((tmp_path / 'v2', old), (tmp_path / 'v3', m)):
        status = stored(s3, db, directory, manifest)
        assert status['status'] == 'stored', status
        shared = service.reference_prefix('synthetic_test', manifest['schema'])
        assert json.loads(s3.objects[f"{status['prefix']}/files.json"])[_parquet.DEPTHS] == f'{shared}/{_parquet.DEPTHS}'
    assert service.reference_prefix('synthetic_test', schema.LEGACY_SCHEMA) != service.reference_prefix('synthetic_test')
