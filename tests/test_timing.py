"""Timing-only reports: validation, accumulation, publication and reading."""
import copy
import datetime as dt
import hashlib
import json

import pytest

import archive_service as service
import briskapi
import briskapi.cli as cli
import briskapi.schema as schema
from briskapi.timing import TimingStats
from test_archive import DB, S3, event
from test_sbi import sbi_session


def report(**changes):
    stats = TimingStats('sbi_live')
    for batch in sbi_session():
        stats.add(batch)
    return {**stats.report('anon-1234abcd', 'CC0-1.0'), **changes}


@pytest.fixture(autouse=True)
def keyed(monkeypatch):
    monkeypatch.setattr(service, 'QUOTA_KEY', b'test-key')


def test_accumulated_report_is_valid_and_market_free():
    r = report()
    assert schema.validate_timing(r) is r and r['client_version'].count('.') == 2
    assert set(r) == schema.TIMING_KEYS
    short = TimingStats('sbi_live')
    for batch in sbi_session(frames=50):
        short.add(batch)
    assert short.report('a', 'CC0-1.0') is None and TimingStats('sbi_live').report('a', 'CC0-1.0') is None


@pytest.mark.parametrize('change', [
    {'extra': 1}, {'schema': 'v0'}, {'source': 'historical_mock'}, {'trading_date': '2026-03-11'},
    {'trading_date': '20260230'}, {'first_minute': '25:00'}, {'first_minute': '15:00', 'last_minute': '09:00'},
    {'frames': 99}, {'stalls': 10**6}, {'client_version': 'latest'}, {'contributor': 'a b'}, {'license': 'MIT'},
    {'decode_ms': {'p50': 1, 'p90': 1, 'p99': 1}}, {'decode_ms': {'p50': 2, 'p90': 1, 'p99': 3, 'max': 4}},
    {'decode_ms': {'p50': 1, 'p90': 1, 'p99': 1, 'max': 10**6}}, {'decode_ms': {'p50': 0.0001, 'p90': 1, 'p99': 1, 'max': 1}},
    {'source_age_ms': {'p50': -10**7, 'p90': 1, 'p99': 1, 'max': 1}}, {'interarrival_ms': None},
])
def test_invalid_reports_rejected(change):
    with pytest.raises(ValueError):
        schema.validate_timing(report(**change))


def test_report_date_window():
    r = report()
    schema.validate_timing(r, today=dt.date(2026, 3, 20))
    for today in (dt.date(2026, 3, 10), dt.date(2026, 5, 1)):
        with pytest.raises(ValueError, match='date'):
            schema.validate_timing(r, today=today)


def test_service_publishes_canonical_report(monkeypatch):
    s3, db = S3(), DB()
    r = report()
    monkeypatch.setattr(service, 'dt', type('dt', (), {'datetime': type('d', (), {
        'now': staticmethod(lambda tz: dt.datetime(2026, 3, 12, tzinfo=tz))}), 'timezone': dt.timezone}))
    published = json.loads(service.api(event(body={'timing': r}), s3, db)['body'])
    body = schema.encode(r)
    assert published == {'status': 'published', 'key': f'timing/20260311/{hashlib.sha256(body).hexdigest()}.json'}
    assert s3.objects[published['key']] == body
    # Same report again: same object, no error.
    assert json.loads(service.api(event(body={'timing': r}), s3, db)['body']) == published
    # e-shiten sessions report the same way (to a separate archive, so the listing below stays one report).
    e = report(source='eshiten_live')
    assert json.loads(service.api(event(body={'timing': e}), S3(), db)['body'])['status'] == 'published'
    assert {c['Key']['id']['S'].split(':')[0][:7] for c in db.calls} >= {'timing-', 'global-'}
    entries = list(cli.timing_reports(s3, 'bucket'))
    assert entries == [(published['key'], r)] and list(cli.timing_reports(s3, 'bucket', '20260311'))
    with pytest.raises(ValueError):
        list(cli.timing_reports(s3, 'bucket', '../x'))
    archive = briskapi.Archive(s3=s3)
    assert archive.timing()[0]['frames'] == 150
    bad = copy.deepcopy(r); bad['frames'] = 1
    monkeypatch.setattr(service, 'clients', lambda: (s3, db))
    assert service.handler(event(body={'timing': bad}), None)['statusCode'] == 400
    stale = {**r, 'trading_date': '20250101'}
    assert service.handler(event(body={'timing': stale}), None)['statusCode'] == 400
    db.reject = True
    assert service.handler(event(body={'timing': report(contributor='other')}), None)['statusCode'] == 400


def test_contribute_timing_posts_report(monkeypatch):
    calls = []
    monkeypatch.setattr(cli, 'request_json', lambda url, data: calls.append((url, data)) or {'status': 'published'})
    assert cli.contribute_timing({'x': 1}, 'https://api.test') == {'status': 'published'}
    assert calls == [('https://api.test', {'timing': {'x': 1}})]
