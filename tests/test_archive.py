import copy
import datetime as dt
import hashlib
import hmac
import io
import json
import os
import re
import shutil
import urllib.error
from pathlib import Path
import uuid
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

import briskapi.schema as schema
import archive_service as service
import briskapi.cli as cli
from briskapi import _parquet
from briskapi._recording import Recording


def batches(source='synthetic_test'):
    b = schema.synthetic_recording()
    b[0]['source'] = source
    return b

def stream(bs):
    return io.BytesIO(b''.join(map(schema.encode, bs)))

def raw(bs):
    # Decoder-style output: unsorted keys and issues, full-precision timings.
    out = []
    for b in copy.deepcopy(bs):
        if b['type'] == 'quotes':
            b['replay_lateness_ms'] = 0.1234567
        out.append(json.dumps(dict(reversed(list(b.items())))).encode() + b'\n')
    return b''.join(out)

def canon(bs):
    return b''.join(schema.canonical_lines(io.BytesIO(raw(bs))))

def reference(bs):
    return {bs[0]['source']: schema.build_reference(stream(bs))}

SECRET = 'brisk-id-v1-' + 'a' * 26
OTHER = 'brisk-id-v1-' + 'b' * 26
OWNER = schema.owner(SECRET)
API = 'https://api.test'

@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.setenv('BRISK_IDENTITY', SECRET)
    monkeypatch.delenv('BRISK_UPLOAD', raising=False)
    monkeypatch.setattr(service, 'QUOTA_KEY', b'test-key')
    monkeypatch.setattr(cli, 'interactive', lambda: False)

@pytest.fixture
def packaged(tmp_path):
    events = tmp_path / 'source.jsonl'
    events.write_bytes(raw(batches()))
    directory = tmp_path / 'package'
    manifest = cli.package(events, directory)
    return directory, manifest

class Pages:
    def __init__(self, pages): self.pages = pages
    def paginate(self, **kw): return self.pages(**kw)

class S3:
    """A versioned bucket: staging uploads are addressed by version; deleting a current object without
    a version leaves a delete marker and keeps the object as a noncurrent version ('kept', in `retained`
    with the time of its deletion), which copy_object can restore."""
    def __init__(self):
        self.objects = {}
        self.classes = {}   # key -> the storage class it was written with (None: the bucket's default)
        self.versions = {}
        self.retained = {}
        self.deleted = []
        self.now = dt.datetime.now(dt.timezone.utc)
    def put_object(self, Bucket, Key, Body, **kw):
        if kw.get('IfNoneMatch') and Key in self.objects:
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
        self.objects[Key] = Body.read() if hasattr(Body, 'read') else Body
        self.classes[Key] = kw.get('StorageClass')
        return {}
    def get_object(self, Bucket, Key, VersionId=None, Range=None):
        try:
            data = (self.retained[Key][0] if VersionId == 'kept' else self.versions[(Key, VersionId)]) if VersionId \
                else self.objects[Key]
        except KeyError:
            raise ClientError({'Error': {'Code': 'NoSuchVersion' if VersionId else 'NoSuchKey'}}, 'GetObject')
        if Range:
            self.ranges = getattr(self, 'ranges', 0) + 1
            low, high = map(int, Range.removeprefix('bytes=').split('-'))
            data = data[low:high + 1]
        return {'Body': io.BytesIO(data), 'ContentLength': len(data)}
    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        return {'ContentLength': len(self.objects[Key])}
    def delete_object(self, Bucket, Key, VersionId=None):
        self.deleted.append((Key, VersionId))
        if VersionId is None:
            if Key in self.objects:
                self.retained[Key] = (self.objects.pop(Key), self.now)
        elif self.versions.pop((Key, VersionId), None) is None:
            raise ClientError({'Error': {'Code': 'NoSuchVersion'}}, 'DeleteObject')
    def generate_presigned_post(self, **kw):
        self.post_args = kw
        return {'url': 'https://example.test', 'fields': {'key': kw['Key']}}
    def generate_presigned_url(self, method, Params, ExpiresIn):
        assert method == 'get_object' and ExpiresIn == service.URL_SECONDS
        return 'https://s3.test/' + Params['Key']
    def copy_object(self, Bucket, Key, CopySource):
        source = CopySource['Key']
        self.objects[Key] = self.retained[source][0] if CopySource.get('VersionId') == 'kept' else self.objects[source]
    def get_paginator(self, name):
        def objects(Bucket, Prefix, StartAfter=''):
            keys = sorted(k for k in self.objects if k.startswith(Prefix) and k > StartAfter)
            return [{'Contents': [{'Key': k} for k in keys[:1]]}, {'Contents': [{'Key': k} for k in keys[1:]]}]
        def versions(Bucket, Prefix):
            older = dt.timedelta(seconds=1)
            return [{'Versions': [{'Key': k, 'VersionId': 'current', 'IsLatest': True, 'LastModified': self.now}
                                  for k in sorted(self.objects) if k.startswith(Prefix)]
                                 + [{'Key': k, 'VersionId': 'kept', 'IsLatest': False, 'LastModified': when - older}
                                    for k, (_, when) in sorted(self.retained.items()) if k.startswith(Prefix)],
                     'DeleteMarkers': [{'Key': k, 'IsLatest': k not in self.objects, 'LastModified': when}
                                       for k, (_, when) in sorted(self.retained.items()) if k.startswith(Prefix)]}]
        return Pages({'list_objects_v2': objects, 'list_object_versions': versions}[name])

class DB:
    def __init__(self, enforce=False):
        self.calls = []; self.versions = {}; self.reject = False
        self.enforce, self.used = enforce, {}   # with `enforce`, quota counters behave like DynamoDB's
    def update_item(self, **kw):
        self.calls.append(kw)
        key = kw['Key']['id']['S']; values = kw['ExpressionAttributeValues']
        if ':version' in values:
            version = values[':version']['S']
            if key in self.versions and self.versions[key] != version:
                raise ClientError({'Error': {'Code': 'ConditionalCheckFailedException'}}, 'UpdateItem')
            self.versions[key] = version
        if self.reject or (self.enforce and ':remaining' in values
                           and self.used.get(key, 0) > int(values[':remaining']['N'])):
            raise ClientError({'Error': {'Code': 'ConditionalCheckFailedException'}}, 'UpdateItem')
        if ':amount' in values:
            self.used[key] = self.used.get(key, 0) + int(values[':amount']['N'])
        return {}

def event(method='POST', body=None, query='', secret=SECRET, ip='127.0.0.1'):
    return {'requestContext': {'http': {'method': method, 'sourceIp': ip}},
            'headers': {'authorization': f'Bearer {secret}'} if secret else {},
            'body': json.dumps(body), 'rawQueryString': query}

def call(s3, db, body, secret=SECRET):
    """One API request; returns (status code, body)."""
    try:
        response = service.api(event(body=body, secret=secret), s3, db)
    except service.NotFound:
        return 404, {'error': 'Not found'}
    except service.INVALID as e:
        return 400, {'error': str(e)}
    return response['statusCode'], json.loads(response['body'])

def upload(s3, manifest, directory, ticket, version='v1', data=None):
    key = f'incoming/{ticket}/recording.zip'
    s3.versions[(key, version)] = data if data is not None else _parquet.pack(directory)
    return {'s3': {'bucket': {'name': service.BUCKET}, 'object': {'key': key, 'versionId': version}}}

def jsonl(directory):
    return b''.join(Recording(directory).lines())

def ticket_for(s3, db, m, secret=SECRET, part=None):
    return call(s3, db, {'action': 'upload', 'manifest': m, 'part': part}, secret)[1]['ticket']

def part_of(directory):
    """A package's timing table alone, as a demo replay sends it, and its part description."""
    timing = _parquet.pack(directory, (_parquet.BATCHES,))
    return timing, {'sha256': _parquet.digest(timing), 'bytes': len(timing)}

def kept(s3, prefix):
    """What the bucket holds below a recording's prefix, by path below it."""
    return {k.removeprefix(prefix + '/'): v for k, v in s3.objects.items() if k.startswith(prefix + '/')}

@pytest.fixture
def online(monkeypatch):
    """The client against the service in this process: API requests and the download URLs it signs."""
    s3, db = S3(), DB()
    def request_json(url, data=None, secret=None):
        if data is None:
            return json.loads(service.api(event('GET', query=url.split('?', 1)[1]), s3, db)['body'])
        code, body = call(s3, db, data, secret)
        if code == 409:
            raise urllib.error.HTTPError(url, code, body['error'], {}, None)
        if code >= 400:
            raise ValueError(body['error'])
        return body
    monkeypatch.setattr(cli, 'request_json', request_json)
    monkeypatch.setattr(cli.urllib.request, 'urlopen', lambda url, timeout, **_: io.BytesIO(s3.objects[url.removeprefix('https://s3.test/')]))
    return s3, db

def stored(s3, db, directory, m):
    ticket = ticket_for(s3, db, m)
    service.ingest(upload(s3, m, directory, ticket), s3, db)
    return service.json_get(s3, f'incoming/{ticket}/status.json')

def test_upload_store_and_pull_your_own(packaged, tmp_path, online):
    directory, m = packaged; s3, db = online
    ticket = ticket_for(s3, db, m)
    assert s3.post_args['Conditions'][-1] == ['content-length-range', m['bytes'], m['bytes']]
    assert json.loads(s3.objects[f'incoming/{ticket}/owner.json']) == {'owner': OWNER}
    rec = upload(s3, m, directory, ticket)
    service.ingest(rec, s3, db)
    status = json.loads(service.api(event('GET', query='ticket=' + ticket), s3, db)['body'])
    assert status['status'] == 'stored' and status['prefix'] == f'archive/{OWNER}/{schema.recording_name(m)}'
    assert status['prefix'].startswith(f'archive/{OWNER}/20210927/000000-000000-')  # the source clocks, HHMMSS
    # The validated staging upload is gone. The replay's market tables become the archive's one copy;
    # the recording keeps its own timing table and where its other tables are.
    assert not s3.versions and s3.deleted == [(f'incoming/{ticket}/recording.zip', 'v1')]
    # Tables are stored in Intelligent-Tiering (seldom read once written); manifests and the like as the bucket has it.
    tables = [k for k in s3.objects if k.endswith('.parquet')]
    assert tables and all(s3.classes[k] == 'INTELLIGENT_TIERING' for k in tables)
    assert s3.classes[status['prefix'] + '/manifest.json'] is None
    shared = service.reference_prefix('synthetic_test')
    assert sorted(kept(s3, status['prefix'])) == [_parquet.BATCHES, 'files.json', 'manifest.json']
    assert json.loads(s3.objects[status['prefix'] + '/files.json']) == {
        name: f"{shared if name in _parquet.MARKET else status['prefix']}/{name}" for name in _parquet.FILES}
    assert sorted(kept(s3, shared)) == sorted([*_parquet.MARKET, 'complete.json'])
    assert status['sha256'] == m['sha256']
    entries = list(cli.manifests(API))
    assert len(entries) == 1 and entries[0] == (status['prefix'], m)
    result = cli.pull(API, status['prefix'], tmp_path / 'download')
    assert result['sha256'] == status['sha256'] and _parquet.pack(tmp_path / 'download') == _parquet.pack(directory)
    assert sorted(p.name for p in (tmp_path / 'download').rglob('*') if p.is_file()) == \
        ['BriskBatch.parquet', 'equity.parquet', 'manifest.json', 'order_book_depths.parquet', 'trades.parquet']
    assert jsonl(tmp_path / 'download') == canon(batches())
    # At-least-once delivery, and extra POSTs with the same upload form, cannot
    # change a stored recording or leave data in staging.
    service.ingest(rec, s3, db)
    service.ingest(upload(s3, m, directory, ticket, 'v2', b'bad'), s3, db)
    assert not s3.versions
    assert json.loads(service.api(event('GET', query='ticket=' + ticket), s3, db)['body'])['status'] == 'stored'
    with pytest.raises(ValueError, match='already exists'):
        cli.pull(API, status['prefix'], tmp_path / 'download')

def test_a_replays_market_data_is_stored_once(packaged, tmp_path, online, monkeypatch):
    directory, m = packaged; s3, db = online
    # Until the archive holds the market data, a timing-only upload is turned away (409: send it all).
    assert call(s3, db, {'action': 'upload', 'manifest': m, 'part': part_of(directory)[1]})[0] == 409
    stored(s3, db, directory, m)
    # Another replay, with its own timing, by another owner: only its timing table is sent and kept.
    replay = batches(); replay[1]['decode_ns'] = 2345
    events = tmp_path / 'replay.jsonl'; events.write_bytes(raw(replay))
    other = tmp_path / 'replay'
    m2 = cli.package(events, other)
    timing, part = part_of(other)
    code, issued = call(s3, db, {'action': 'upload', 'manifest': m2, 'part': part}, OTHER)
    assert code == 201 and s3.post_args['Conditions'][-1] == ['content-length-range', len(timing), len(timing)]
    service.ingest(upload(s3, m2, other, issued['ticket'], data=timing), s3, db)
    status = service.json_get(s3, f"incoming/{issued['ticket']}/status.json")
    assert status['status'] == 'stored' and status['sha256'] == m2['sha256'] != m['sha256']
    assert sorted(kept(s3, status['prefix'])) == [_parquet.BATCHES, 'files.json', 'manifest.json']
    assert sum(key.endswith(_parquet.DEPTHS) for key in s3.objects) == 1
    monkeypatch.setenv('BRISK_IDENTITY', OTHER)
    cli.pull(API, status['prefix'], tmp_path / 'download')
    assert jsonl(tmp_path / 'download') == jsonl(other) and _parquet.pack(tmp_path / 'download') == _parquet.pack(other)
    # A timing table that is not this replay's (or a part for a source without one) is refused.
    short = tmp_path / 'short'
    rewrite_copy = lambda table: table.slice(0, 2)  # noqa: E731
    shutil.copytree(other, short); rewrite(short, _parquet.BATCHES, rewrite_copy)
    timing, part = part_of(short)
    ticket = ticket_for(s3, db, m2, part=part)
    service.ingest(upload(s3, m2, short, ticket, data=timing), s3, db)
    assert service.json_get(s3, f'incoming/{ticket}/status.json')['status'] == 'rejected'
    broker = {**m2, 'summary': {**m2['summary'], 'source': 'brisk_live'}}
    assert call(s3, db, {'action': 'upload', 'manifest': broker, 'part': part}) == \
        (400, {'error': 'Only a demo or synthetic replay can send its timing alone'})

def test_owners_see_only_their_own(packaged, tmp_path, online, monkeypatch):
    directory, m = packaged; s3, db = online
    status = stored(s3, db, directory, m)
    prefix = status['prefix']
    for action in ({'action': 'download', 'prefix': prefix}, {'action': 'delete', 'prefix': prefix}):
        assert call(s3, db, action, OTHER) == (400, {'error': 'Not your upload'})
        assert call(s3, db, action, None)[0] == 400 and call(s3, db, action, 'brisk-id-v1-short')[0] == 400
    listing = {'action': 'list', 'kind': 'recordings', 'date': None, 'after': None}
    assert call(s3, db, listing, OTHER) == (200, {'items': [], 'next': None})
    monkeypatch.setenv('BRISK_IDENTITY', OTHER)
    assert list(cli.manifests(API)) == []
    with pytest.raises(ValueError, match='Not your upload'):
        cli.pull(API, prefix, tmp_path / 'other')
    monkeypatch.setenv('BRISK_IDENTITY', SECRET)
    # The same recording uploaded by another owner is kept apart.
    ticket = ticket_for(s3, db, m, OTHER)
    service.ingest(upload(s3, m, directory, ticket), s3, db)
    assert service.json_get(s3, f'incoming/{ticket}/status.json')['prefix'] == prefix.replace(OWNER, schema.owner(OTHER))
    assert [p for p, _ in cli.manifests(API)] == [prefix]
    # Deleting removes it from the owner's archive, the manifest (the listing's marker) first; the
    # operator's copy stays as a noncurrent version, and the replay's shared market tables stay.
    assert cli.delete(API, prefix) == {'status': 'deleted', 'removed': 3,
                                       'note': 'Removed from your archive; `brisk restore` brings it back within 30 days.'}
    own = [prefix + '/manifest.json', f'{prefix}/{_parquet.BATCHES}', prefix + '/files.json']
    assert s3.deleted[-3:] == [(key, None) for key in own] and set(s3.retained) == set(own)
    assert f"{service.reference_prefix('synthetic_test')}/{_parquet.DEPTHS}" in s3.objects
    assert list(cli.manifests(API)) == [] and call(s3, db, {'action': 'delete', 'prefix': prefix})[0] == 404
    assert call(s3, db, {'action': 'download', 'prefix': prefix})[0] == 404
    assert call(s3, db, {'action': 'download', 'prefix': '../x'})[0] == 400
    assert call(s3, db, {'action': 'list', 'kind': 'everything', 'date': None, 'after': None})[0] == 400
    assert call(s3, db, {'action': 'list', 'kind': 'recordings', 'date': 'today', 'after': None})[0] == 400
    assert call(s3, db, {'action': 'list', 'kind': 'recordings', 'date': None, 'after': 'archive/x'})[0] == 400

def test_prune_frees_disk_once_the_archive_holds_it(packaged, tmp_path, online, harness, capsys):
    directory, m = packaged; s3, db = online
    (directory / 'notes.txt').write_text('mine')
    with pytest.raises(ValueError, match='does not hold'):
        cli.prune(API, directory)
    status = stored(s3, db, directory, m)
    trades = directory / _parquet.TRADES
    original = trades.read_bytes()
    trades.write_bytes(original + b'x')
    with pytest.raises(ValueError, match='differs from its manifest'):
        cli.prune(API, directory)
    trades.write_bytes(original)
    size = sum((directory / name).stat().st_size for name in (*_parquet.FILES, 'manifest.json'))
    harness('prune', str(directory))
    assert json.loads(capsys.readouterr().out) == {'pruned': str(directory), 'bytes': size, 'prefix': status['prefix']}
    # Only the package goes; anything else in the folder stays, and pull brings the recording back.
    assert [p.name for p in directory.rglob('*')] == ['notes.txt']
    with pytest.raises(ValueError, match='no manifest'):
        cli.prune(API, directory)
    cli.pull(API, status['prefix'], tmp_path / 'back')
    assert (tmp_path / 'back' / _parquet.TRADES).read_bytes() == original
    # A replay whose market tables differ from the archive's copy byte for byte (another writer), not in content,
    # is found by its own timing table.
    local = tmp_path / 'local'
    shutil.copytree(tmp_path / 'back', local)
    rewrite(local, _parquet.EQUITY, lambda table: table, use_dictionary=False)
    blob = _parquet.pack(local)
    assert _parquet.digest(blob) != status['sha256']
    (local / 'manifest.json').write_text(json.dumps({**json.loads((local / 'manifest.json').read_text()),
                                                     'sha256': _parquet.digest(blob), 'bytes': len(blob)}))
    assert cli.prune(API, local)['prefix'] == status['prefix'] and not local.exists()
    shutil.copytree(tmp_path / 'back', local)
    # Another replay's timing (the same widths, so the same summary) is not this one.
    rewrite(local, _parquet.BATCHES, lambda table: replace(table, 'decode_ns', pa.array(
        [None if v is None else v + 1 for v in table.column('decode_ns').to_pylist()], table.schema.field('decode_ns').type)))
    blob = _parquet.pack(local)
    (local / 'manifest.json').write_text(json.dumps({**json.loads((local / 'manifest.json').read_text()),
                                                     'sha256': _parquet.digest(blob), 'bytes': len(blob)}))
    with pytest.raises(ValueError, match='does not hold'):
        cli.prune(API, local)

def test_samples_copy_only_the_legacy_wire_capture():
    import importlib.util
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('deploy', root / 'infra/deploy.py')
    deploy = importlib.util.module_from_spec(spec); spec.loader.exec_module(deploy)
    wire = json.dumps({'schema': 'brisk-sbi-wire-jsonl-v1'}).encode()
    class Bucket(S3):
        def head_object(self, Bucket, Key):
            if Key not in self.objects:
                raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        def copy_object(self, Bucket, Key, CopySource):
            self.objects[Key] = self.objects[CopySource['Key']]
    bucket = Bucket()
    bucket.objects.update({'archive/20260311/aaa/manifest.json': wire, 'archive/20260311/aaa/events.jsonl.gz': b'gz',
                           'archive/20210927/bbb/manifest.json': json.dumps({'schema': schema.SCHEMA}).encode(),
                           f'archive/{OWNER}/20260311/ccc/manifest.json': wire})
    assert deploy.publish_samples(bucket, 'b') == ['samples/20260311/aaa/events.jsonl.gz', 'samples/20260311/aaa/manifest.json']
    assert bucket.objects['samples/20260311/aaa/events.jsonl.gz'] == b'gz'
    assert 'archive/20260311/aaa/events.jsonl.gz' in bucket.objects
    assert deploy.publish_samples(bucket, 'b') == []

def test_deleted_uploads_can_be_restored_for_30_days(packaged, tmp_path, online, monkeypatch, harness, capsys):
    directory, m = packaged; s3, db = online
    prefix = stored(s3, db, directory, m)['prefix']
    from test_timing import report
    timing = report()
    key = f"timing/{OWNER}/{timing['trading_date']}/{hashlib.sha256(schema.encode(timing)).hexdigest()}.json"
    service.json_put(s3, key, timing)
    assert list(cli.deleted(API)) == []
    cli.delete(API, prefix); cli.delete(API, key)
    assert list(cli.manifests(API)) == [] and list(cli.timing_reports(API)) == []
    gone = list(cli.deleted(API))
    assert [item.get('prefix') or item['key'] for item in gone] == [prefix, key]
    assert gone[0]['manifest'] == m and gone[1]['report'] == timing
    until = dt.datetime.fromisoformat(gone[0]['restore_until']) - dt.datetime.fromisoformat(gone[0]['deleted_at'])
    assert until == dt.timedelta(days=30)
    # Only its owner can restore it; the manifest comes back last.
    assert call(s3, db, {'action': 'restore', 'prefix': prefix}, OTHER) == (400, {'error': 'Not your upload'})
    from briskapi import Archive
    monkeypatch.setattr(cli, 'settings', lambda path=None: {'api_url': API})
    assert len(Archive().deleted()) == 2
    assert Archive().restore(prefix) == {'status': 'restored', 'restored': 3}
    harness('restore', key)
    assert json.loads(capsys.readouterr().out) == {'status': 'restored', 'restored': 1}
    assert [p for p, _ in cli.manifests(API)] == [prefix] and [k for k, _ in cli.timing_reports(API)] == [key]
    cli.pull(API, prefix, tmp_path / 'back')
    assert _parquet.pack(tmp_path / 'back') == _parquet.pack(directory)
    harness('list', '--deleted')
    assert capsys.readouterr().out == ''
    # After 30 days it can't come back (and the bucket's lifecycle drops it a day later).
    cli.delete(API, prefix)
    s3.retained = {k: (data, when - dt.timedelta(days=30, minutes=1)) for k, (data, when) in s3.retained.items()}
    assert list(cli.deleted(API)) == []
    assert call(s3, db, {'action': 'restore', 'prefix': prefix}) == (404, {'error': 'Not found'})

def test_listing_pages(packaged, online, monkeypatch):
    directory, m = packaged; s3, db = online
    monkeypatch.setitem(service.PAGE, 'recordings', 2)
    prefixes = []
    for k in range(5):
        sha = f'{k}' * 64
        prefix = f"archive/{OWNER}/{schema.recording_name({**m, 'sha256': sha})}"
        service.json_put(s3, prefix + '/manifest.json', {**m, 'sha256': sha})
        prefixes.append(prefix)
    first = call(s3, db, {'action': 'list', 'kind': 'recordings', 'date': None, 'after': None})[1]
    assert [i['prefix'] for i in first['items']] == prefixes[:2] and first['next'] == prefixes[1] + '/manifest.json'
    assert [p for p, _ in cli.manifests(API)] == prefixes == [p for p, _ in cli.manifests(API, '20210927')]
    assert list(cli.manifests(API, '20210928')) == []

def test_zip_container_never_stored(packaged):
    directory, m = packaged; s3 = S3(); db = DB()
    payload = b'ARBITRARY-PAYLOAD'
    stuffed = io.BytesIO()
    with zipfile.ZipFile(stuffed, 'w') as z:
        z.comment = payload
        for name in _parquet.FILES:
            z.writestr(zipfile.ZipInfo(name, date_time=(2020, 2, 2, 2, 2, 2)), (directory / name).read_bytes())
    data = stuffed.getvalue()
    m = {**m, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}
    ticket = ticket_for(s3, db, m)
    service.ingest(upload(s3, m, directory, ticket, data=data), s3, db)
    status = service.json_get(s3, f'incoming/{ticket}/status.json')
    assert status['status'] == 'stored' and status['sha256'] == _parquet.digest(_parquet.pack(directory)) != m['sha256']
    assert not [key for key, value in s3.objects.items() if payload in value]


def rewrite(directory, name, change, **options):
    """Rewrite one table of a package after `change` (a function of the table)."""
    path = directory / name
    table = change(pq.read_table(path))
    pq.write_table(table, path, **{**_parquet.WRITE, 'row_group_size': _parquet.ROW_GROUP, **options})


def replace(table, name, values):
    return table.set_column(table.schema.get_field_index(name), table.schema.field(name), values)


@pytest.mark.parametrize('name,change,error', [
    # Values the decoder does not read are checked against the content they derive from.
    (_parquet.DEPTHS, lambda t: replace(t, 'flags', pa.array([255] * len(t), pa.uint8())), 'Non-canonical'),
    (_parquet.EQUITY, lambda t: replace(t, 'currency', pa.array(['payload'] * len(t))), 'Non-canonical'),
    (_parquet.EQUITY, lambda t: replace(t, 'info', pa.array(['{"payload":1}'] * len(t))), 'Non-canonical'),
    (_parquet.BATCHES, lambda t: replace(t, 'type', pa.array(['payload'] * len(t))), 'Non-canonical'),
    # The zero price sentinel has one encoding: null.
    (_parquet.DEPTHS, lambda t: replace(t, 'open_price', pa.array([0] * len(t)).cast(_parquet.DECIMAL)), 'Non-canonical'),
    # Rows in one order only, and no metadata beyond the schema's.
    (_parquet.DEPTHS, lambda t: t.take([1, 0]), 'sorted'),
    (_parquet.BATCHES, lambda t: t.replace_schema_metadata({**t.schema.metadata, b'payload': b'1'}), 'schema'),
    (_parquet.DEPTHS, lambda t: replace(t, 'seq', pa.array([1] * len(t), pa.uint64())), 'sorted'),
    (_parquet.TRADES, lambda t: replace(t, 'trade_id', pa.array(['payload'] * len(t))), 'Non-canonical'),
    (_parquet.TRADES, lambda t: replace(t, 'aggressor_side', pa.array(['SELL'] * len(t)).dictionary_encode().cast(
        pa.dictionary(pa.int8(), pa.string()))), 'Non-canonical'),
])
def test_uploaded_tables_must_be_canonical(packaged, name, change, error):
    directory, m = packaged
    two = copy.deepcopy(batches())
    two[0]['quotes'].append(dict(two[0]['quotes'][0], issue_id=1, code='0001'))
    two[0]['master'].append(dict(two[0]['master'][0], issue_id=1, code='0001'))
    two[0]['market_issue_count'] = 2
    _parquet.write(two, directory / 'two')
    rewrite(directory / 'two', name, change)
    columns = _parquet.Columns.load(directory / 'two')
    with pytest.raises(ValueError, match=error):
        _parquet.check(directory / 'two')
        _parquet.verify_tables(directory / 'two', columns)


@pytest.mark.parametrize('options,error', [({}, None), ({'compression': 'snappy', 'compression_level': None}, 'compression'),
                                           ({'row_group_size': 1}, 'row groups')])
def test_writer_choices(packaged, options, error):
    directory, m = packaged
    rewrite(directory, _parquet.DEPTHS, lambda t: t, **options)
    if error:
        with pytest.raises(ValueError, match=error):
            _parquet.check(directory)
    else:
        _parquet.check(directory)


def test_file_bytes_outside_the_tables_are_refused(packaged):
    directory, m = packaged
    path = directory / _parquet.EQUITY
    data = path.read_bytes()
    path.write_bytes(data[:4] + b'payload!' + data[4:])
    with pytest.raises((ValueError, OSError)):
        _parquet.check(directory)

@pytest.mark.parametrize('mutation', [
    lambda b: b[1].update(seq=3),
    lambda b: b[0].update(source='live'),
    lambda b: b[0].update(trading_date='20250230'),
    lambda b: b[0].update(trading_date='20210928'),
    lambda b: b[0].update(quotes=[]),
    lambda b: b[0].update(exchange_delay_ms=0),
    lambda b: b[0].update(source_timestamp_origin='exchange'),
    lambda b: b[0].update(input_transport={'token': 'secret'}),
    lambda b: b[0].update(input_transport={'kind': 'https_recorded_assets', 'origin': 'https://bad.test', 'asset_fetch_ms': 1}),
    lambda b: b[0].update(market_issue_count=2),
    lambda b: b[0].pop('input_transport'),
    lambda b: b[0]['master'].append(b[0]['master'][0]),
    lambda b: b[0]['master'][0].update(name='payload'),
    lambda b: b[0]['master'][0].update(name='x' * 300),
    lambda b: b[0]['master'][0].update(market=1),
    lambda b: b[0]['master'][0].pop('lot_size'),
    lambda b: b[1]['quotes'][0].update(code='1111'),
    lambda b: b[1]['quotes'][0].update(frame=0),
    lambda b: b[1]['quotes'][0].update(source_time_us=111),
    lambda b: b[1]['quotes'][0].update(indicative_price10=9999),
    lambda b: b[1]['quotes'][0].update(token='secret'),
    lambda b: b[1]['quotes'][0].pop('volume'),
    lambda b: b[1]['quotes'].append(b[1]['quotes'][0]),
    lambda b: b[1].update(source_time_us=99),
    lambda b: b[1].update(source_time_us=111),
    lambda b: b[1].update(type='bootstrap'),
    lambda b: b[1].update(master=[]),
    lambda b: b[1].update(decode_ns=-1),
    lambda b: b[1].update(decode_ns=10**12),
    lambda b: b[1].update(received_unix_ms=1),
    lambda b: b[1].update(replay_lateness_ms=-1),
    lambda b: b[1].update(replay_lateness_ms=0.1234567),
    lambda b: b[1].update(replay_lateness_ms=-0.0),
    lambda b: b[1].update(replay_lateness_ms=float('nan')),
    lambda b: b[2].update(quotes=b[0]['quotes']),
    lambda b: b[2].update(frames=9),
    lambda b: b[2].update(quote_updates=9),
    lambda b: b[2].update(replay_wall_ms=10**9),
    lambda b: b[2].update(type='bogus'),
    lambda b: b.pop(),
    lambda b: b.append(b[-1]),
])
def test_reject_invalid_stream(mutation):
    b = batches(); mutation(b)
    with pytest.raises((ValueError, KeyError)):
        schema.validate_stream(stream(b))

def paced(n=4):
    # A longer synthetic shape for clock checks, with its own reference.
    b = batches('historical_mock')
    quote = b[1]['quotes'][0]
    middle = [dict(b[1], seq=i, source_time_us=110 + i, received_unix_ms=1632700800000 + i, trades=b[1]['trades'] if i == 1 else [],
                   quotes=[dict(quote, frame=1 + i, max_frame=1 + i, source_time_us=110 + i)]) for i in range(1, n)]
    end = dict(b[2], seq=n, source_time_us=110 + n - 1, frames=n, quote_updates=n - 1)
    return [b[0], *middle, end]

@pytest.mark.parametrize('change,valid', [
    (lambda b: None, True),
    (lambda b: [x.update(replay_lateness_ms=None) for x in b[1:-1]], True),
    (lambda b: b[2].update(replay_lateness_ms=None), False),
    (lambda b: b[2].update(received_unix_ms=b[1]['received_unix_ms'] - 1), False),
    (lambda b: b[3].update(received_unix_ms=b[1]['received_unix_ms'] + schema.MAX_SESSION_MS + 1), False),
])
def test_local_clock_rules(change, valid):
    b = paced(); ref = reference(paced()); change(b)
    if valid:
        assert schema.validate_stream(stream(b), ref)['batches'] == 5
    else:
        with pytest.raises(ValueError):
            schema.validate_stream(stream(b), ref)

@pytest.mark.parametrize('encoding', [
    lambda line: line.replace(b',', b', ', 1),
    lambda line: line.replace(b'{', b'{"seq":9,', 1),
    lambda line: line.replace(b'"Synthetic fixture"', b'"Synthetic \\u0066ixture"'),
    lambda line: b'{"type":"bootstrap",' + line[1:].replace(b',"type":"bootstrap"', b''),
    lambda line: line.rstrip(b'\n'),
    lambda line: line.replace(b'"lot_size":100', b'"lot_size":100.0'),
])
def test_reject_noncanonical_bytes(encoding):
    lines = stream(batches()).read().splitlines(True)
    lines[0] = encoding(lines[0])
    with pytest.raises(ValueError):
        schema.validate_stream(io.BytesIO(b''.join(lines)))

def test_decode_limits(monkeypatch):
    monkeypatch.setattr(schema, 'MAX_LINE', 3)
    with pytest.raises(ValueError, match='limit'):
        schema.validate_stream(stream(batches()))
    with pytest.raises(ValueError, match='limit'):
        list(schema.canonical_lines(stream(batches())))
    monkeypatch.setattr(schema, 'MAX_LINE', 1024 * 1024)
    monkeypatch.setattr(schema, 'MAX_EXPANDED', 3)
    with pytest.raises(ValueError, match='limit'):
        schema.validate_stream(stream(batches()))
    monkeypatch.setattr(schema, 'MAX_EXPANDED', 1024 * 1024)
    with pytest.raises(ValueError, match='Nonfinite'):
        schema.validate_stream(io.BytesIO(b'{"seq":NaN}\n'))

def test_reference_rules(monkeypatch):
    b = batches('historical_mock')
    with pytest.raises(ValueError, match='No reference'):
        schema.validate_stream(stream(b), {})
    assert schema.validate_stream(stream(b), reference(b))['source'] == 'historical_mock'
    other = reference(b); other['historical_mock']['issues'] = []
    with pytest.raises(ValueError, match='differs'):
        schema.validate_stream(stream(b), other)
    two = copy.deepcopy(b)
    two[0]['market_issue_count'] = 2
    with pytest.raises(ValueError, match='whole market'):
        schema.build_reference(stream(two))
    committed = schema.REFERENCES['historical_mock']
    assert committed['market_issue_count'] == len(committed['issues']) == 4131 and committed['batches'] == 18002

def test_canonical_lines_and_transport():
    b = batches('historical_mock')
    b[0]['input_transport'] = {'kind': 'https_recorded_assets', 'origin': schema.DEMO_ORIGIN, 'asset_fetch_ms': 123.4567891}
    b[0]['master'].insert(0, b[0]['master'][0] | {'issue_id': 5, 'code': '0005'})
    lines = list(schema.canonical_lines(io.BytesIO(raw(b))))
    bootstrap = json.loads(lines[0])
    assert bootstrap['input_transport']['asset_fetch_ms'] == 123.457
    assert [m['issue_id'] for m in bootstrap['master']] == [0, 5]
    assert json.loads(lines[1])['replay_lateness_ms'] == 0.123
    assert all(line == schema.encode(json.loads(line)) for line in lines)
    b[0]['master'].pop(0); b[0]['market_issue_count'] = 1
    canonical = b''.join(schema.canonical_lines(io.BytesIO(raw(b))))
    ref = {'historical_mock': schema.build_reference(io.BytesIO(canonical))}
    assert schema.validate_stream(io.BytesIO(canonical), ref)
    with pytest.raises(ValueError, match='timing'):
        schema.validate_stream(io.BytesIO(canonical.replace(b'"asset_fetch_ms":123.457', b'"asset_fetch_ms":1000000.0')), ref)
    with pytest.raises(ValueError):
        list(schema.canonical_lines(io.BytesIO(b'[1]\n')))
    with pytest.raises(ValueError):
        list(schema.canonical_lines(io.BytesIO(b'{"quotes":[1]}\n')))

@pytest.mark.parametrize('change', [{'sha256': 'bad'}, {'sha256': 1}, {'bytes': 0}, {'contributor': 'anon'},
    {'summary': {'source': 'live'}}, {'schema': 'raw'}, {'extra': 1}])
def test_manifest_permissions(packaged, change):
    directory, m = packaged; m.update(change)
    with pytest.raises(ValueError):
        schema.validate_manifest(m)

@pytest.mark.parametrize('change', [{'codes': ['bad code']}, {'codes': []}, {'codes': 'x'}, {'batches': -1},
    {'trading_date': 'yesterday'}, {'extra': 1}])
def test_manifest_summary_strict(packaged, change):
    directory, m = packaged; m['summary'].update(change)
    with pytest.raises(ValueError):
        schema.validate_manifest(m)

def test_hash_summary_and_corrupt_package(packaged, tmp_path):
    directory, m = packaged
    bad = copy.deepcopy(m); bad['summary']['batches'] = 9
    with pytest.raises(ValueError, match='Summary'):
        _parquet.inspect_package(directory, bad)
    with pytest.raises(ValueError, match='Summary'):
        _parquet.verify_directory(directory, bad)
    (directory / _parquet.EQUITY).write_bytes(b'not parquet')
    blob = _parquet.pack(directory); m.update(bytes=len(blob), sha256=_parquet.digest(blob))
    with pytest.raises(ValueError):
        _parquet.inspect_package(directory, m)

def zipped(members):
    out = io.BytesIO()
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as z:
        for name, data in members:
            z.writestr(name, data)
    return out.getvalue()

@pytest.mark.parametrize('corrupt', [b'bad', None, 'tampered', 'flipped', 'garbage', 'extra', 'deflated'])
def test_rejection_stays_private(packaged, corrupt, tmp_path):
    directory, m = packaged; s3 = S3(); db = DB()
    ticket = ticket_for(s3, db, m)
    blob = _parquet.pack(directory)
    if corrupt is None:
        m = copy.deepcopy(m); m['summary']['batches'] = 123
        service.json_put(s3, f'incoming/{ticket}/manifest.json', m)
    elif isinstance(corrupt, str):
        if corrupt == 'tampered':
            # Structurally valid but not the reference replay, with a matching manifest.
            b = batches(); b[1]['quotes'][0]['indicative_price10'] = 1250
            _parquet.write(b, tmp_path / 'tampered')
            corrupt = _parquet.pack(tmp_path / 'tampered')
        elif corrupt == 'flipped':
            corrupt = blob[:200] + bytes(b ^ 0xFF for b in blob[200:240]) + blob[240:]
        elif corrupt == 'garbage':
            corrupt = zipped((name, b'PAR1 garbage PAR1') for name in _parquet.FILES)
        elif corrupt == 'extra':
            corrupt = zipped([*((name, (directory / name).read_bytes()) for name in _parquet.FILES), ('payload', b'x')])
        else:
            corrupt = zipped((name, (directory / name).read_bytes()) for name in _parquet.FILES)
        m = {**m, 'sha256': hashlib.sha256(corrupt).hexdigest(), 'bytes': len(corrupt)}
        service.json_put(s3, f'incoming/{ticket}/manifest.json', m)
    service.ingest(upload(s3, m, directory, ticket, data=corrupt), s3, db)
    status = service.json_get(s3, f'incoming/{ticket}/status.json')
    assert status['status'] == 'rejected'
    assert not [k for k in s3.objects if k.startswith('archive/')] and not s3.versions

def test_api_errors_and_handler(packaged, monkeypatch):
    directory, m = packaged; s3 = S3(); db = DB()
    monkeypatch.setattr(service, 'clients', lambda: (s3, db))
    ticket = {'action': 'upload', 'manifest': m, 'part': None}
    assert service.handler(event('DELETE'), None)['statusCode'] == 400
    assert service.handler(event('GET', query='ticket=' + str(uuid.uuid4())), None)['statusCode'] == 404
    assert service.handler(event('GET', query='ticket=wrong'), None)['statusCode'] == 400
    assert service.handler(event(body={'secret': 'bad'}), None)['statusCode'] == 400
    assert service.handler(event(body={**ticket, 'extra': 1}), None)['statusCode'] == 400
    assert service.handler(event(body=ticket, secret=None), None)['statusCode'] == 400
    assert service.handler(event(body=ticket, secret='brisk-id-v1-' + 'g' * 64), None)['statusCode'] == 400
    nested = event(); nested['body'] = '[' * 200000
    assert service.handler(nested, None)['statusCode'] == 400
    db.reject = True
    assert service.handler(event(body=ticket), None)['statusCode'] == 429
    db.reject = False
    monkeypatch.setattr(service, 'QUOTA_KEY', b'')
    assert service.handler(event(body=ticket), None)['statusCode'] == 400
    monkeypatch.setattr(service, 'QUOTA_KEY', b'test-key')
    request = event(body=ticket)
    import base64
    request['isBase64Encoded'] = True; request['body'] = base64.b64encode(request['body'].encode()).decode()
    issued = json.loads(service.handler(request, None)['body'])['ticket']
    assert service.handler({'Records': [upload(s3, m, directory, issued)]}, None) == {'ok': True}
    missing = f'archive/{OWNER}/20210927/000000-000000-' + 'c' * 64
    assert service.handler(event(body={'action': 'download', 'prefix': missing}), None)['statusCode'] == 404
    for part in ({'sha256': 'x', 'bytes': 1}, {'sha256': 'a' * 64, 'bytes': 0}, {'sha256': 'a' * 64}, 'all'):
        assert service.handler(event(body={**ticket, 'part': part}), None)['statusCode'] == 400

def test_quota_key_is_keyed_hash(packaged):
    directory, m = packaged; s3 = S3(); db = DB()
    ticket_for(s3, db, m)
    keyed = 'ip-' + hmac.new(b'test-key', b'127.0.0.1', hashlib.sha256).hexdigest()
    assert [call['Key']['id']['S'].split(':')[0] for call in db.calls[:3]] == [f'requests-{keyed}', keyed, f'owner-{OWNER}']
    assert '127.0.0.1' not in json.dumps(db.calls) and hashlib.sha256(b'127.0.0.1').hexdigest() not in json.dumps(db.calls)
    assert SECRET not in json.dumps(db.calls)

def test_every_request_counts_against_its_address_first(monkeypatch):
    """Guessing identities from one address: every request counts, whatever identity (or none) it names."""
    s3, db = S3(), DB(enforce=True)
    monkeypatch.setattr(service, 'clients', lambda: (s3, db))
    monkeypatch.setattr(service, 'REQUESTS', (4, 60))
    listing = {'action': 'list', 'kind': 'recordings', 'date': None, 'after': None}
    assert service.handler(event('GET', query='ticket=' + str(uuid.uuid4())), None)['statusCode'] == 404
    assert service.handler(event(body=listing, secret='brisk-id-v1-nope'), None)['statusCode'] == 400
    assert service.handler(event(body=listing, secret=None), None)['statusCode'] == 400
    assert service.handler(event(body=listing, secret=OTHER), None)['statusCode'] == 200
    refused = service.handler(event(body=listing), None)
    assert refused['statusCode'] == 429 and 'this address' in json.loads(refused['body'])['error']
    assert service.handler(event('GET', query='ticket=' + str(uuid.uuid4())), None)['statusCode'] == 429
    assert service.handler(event(body=listing, ip='192.0.2.7'), None)['statusCode'] == 200

def test_addresses_are_keyed_hashes_ipv6_by_64(monkeypatch):
    at = lambda ip: service.address(event(ip=ip))  # noqa: E731
    assert at('2001:db8:1:2::1') == at('2001:db8:1:2:ffff:ffff:ffff:ffff') != at('2001:db8:1:3::1')
    assert at('::ffff:192.0.2.1') == at('192.0.2.1') != at('192.0.2.2')
    assert at('192.0.2.1') == hmac.new(b'test-key', b'192.0.2.1', hashlib.sha256).hexdigest()
    assert at('unknown') == hmac.new(b'test-key', b'unknown', hashlib.sha256).hexdigest()
    monkeypatch.setattr(service, 'QUOTA_KEY', b'')
    with pytest.raises(ValueError, match='not configured'):
        at('192.0.2.1')

def test_deletes_and_restores_are_limited(monkeypatch):
    """Each one may rebuild a day of the owner's bars, so they are counted per owner and per address."""
    s3, db = S3(), DB(enforce=True)
    monkeypatch.setattr(service, 'clients', lambda: (s3, db))
    monkeypatch.setattr(service, 'CHANGES', (2, 3600))
    missing = lambda who: f'timing/{schema.owner(who)}/20261013/' + 'c' * 64 + '.json'  # noqa: E731
    change = lambda action, who=SECRET, ip='127.0.0.1': service.handler(  # noqa: E731
        event(body={'action': action, 'prefix': missing(who)}, secret=who, ip=ip), None)['statusCode']
    assert change('delete') == 404 and change('restore') == 404
    assert change('delete') == 429                         # this owner, this address
    assert change('delete', ip='192.0.2.7') == 429         # this owner, elsewhere
    assert change('restore', who=OTHER) == 429             # this address, another owner
    assert change('restore', who=OTHER, ip='192.0.2.8') == 404

@pytest.mark.parametrize('error', ['AccessDenied', 'InternalError'])
def test_cloud_errors_retry(error):
    class Fail:
        def update_item(self, **kw):
            raise ClientError({'Error': {'Code': error}}, 'UpdateItem')
        def put_object(self, **kw):
            raise ClientError({'Error': {'Code': error}}, 'PutObject')
        def delete_object(self, **kw):
            raise ClientError({'Error': {'Code': error}}, 'DeleteObject')
    with pytest.raises(ClientError):
        service.quota(Fail(), 'x', 1, 4, 3600)
    with pytest.raises(ClientError):
        service.json_put(Fail(), 'x', {})
    with pytest.raises(ClientError):
        service.discard(Fail(), 'x', 'v1')

def test_pull_tampered_atomic(packaged, tmp_path, online):
    directory, m = packaged; s3, db = online
    prefix = f"archive/{OWNER}/{schema.recording_name(m)}"
    service.json_put(s3, prefix + '/manifest.json', m)
    service.json_put(s3, prefix + '/files.json', {name: f'{prefix}/{name}' for name in _parquet.FILES})
    for name in _parquet.FILES:
        s3.objects[f'{prefix}/{name}'] = b'bad'
    with pytest.raises(ValueError, match='mismatch'):
        cli.pull(API, prefix, tmp_path / 'bad')
    assert not (tmp_path / 'bad').exists()
    with pytest.raises(ValueError):
        list(cli.manifests(API, '../'))
    with pytest.raises(ValueError):
        cli.pull(API, 'incoming/secret', tmp_path / 'bad')


@pytest.mark.parametrize('invalid', ['schema', 'source', 'sample', 'legacy_probe', 'identity', 'types'])
def test_listing_skips_invalid_entries_and_keeps_valid_ones(packaged, invalid, monkeypatch, capsys, online):
    _, m = packaged
    s3, db = online
    bad = copy.deepcopy(m)
    bad['sha256'] = 'a' * 64
    if invalid == 'schema':
        bad['schema'] = 'brisk-sbi-wire-jsonl-v1'
    elif invalid == 'source':
        bad['summary']['source'] = 'sbi_legacy_capture'
    elif invalid == 'sample':
        bad['schema'] = 'brisk-sbi-wire-jsonl-v1'
        bad['summary']['source'] = 'sample_data'
    elif invalid == 'legacy_probe':
        bad['summary']['batches'] = 2
    elif invalid == 'identity':
        bad['sha256'] = 'b' * 64
    elif invalid == 'types':
        bad['summary']['source'] = []
    service.json_put(s3, f"archive/{OWNER}/{schema.recording_name({**m, 'sha256': 'a' * 64})}/manifest.json", bad)
    prefix = f"archive/{OWNER}/{schema.recording_name(m)}"
    service.json_put(s3, prefix + '/manifest.json', m)
    with pytest.warns(UserWarning, match='Skipping invalid or unsupported archive manifest'):
        assert list(cli.manifests(API)) == [(prefix, m)]
    # Both user-facing catalogs use the same tolerant listing path.
    from briskapi import Archive
    monkeypatch.setattr(cli, 'settings', lambda path=None: {'api_url': API})
    with pytest.warns(UserWarning):
        assert Archive().recordings()[0]['prefix'] == prefix
    with pytest.warns(UserWarning):
        cli.main(['list'])
    assert json.loads(capsys.readouterr().out)['prefix'] == prefix


@pytest.mark.parametrize('change', [{'schema': 'brisk-sbi-wire-jsonl-v1'}, {'source': 'sbi_legacy_capture'},
                                  {'batches': 2}, {'sha256': 'b' * 64}])
def test_pull_rejects_manifest_before_fetching_payload(packaged, tmp_path, change, online):
    _, m = packaged
    s3, db = online
    bad = copy.deepcopy(m)
    prefix = f"archive/{OWNER}/{schema.recording_name(m)}"
    for key, value in change.items():
        (bad if key in {'schema', 'sha256'} else bad['summary'])[key] = value
    service.json_put(s3, prefix + '/manifest.json', bad)
    # No payload object exists: requesting one would raise KeyError.
    with pytest.raises(ValueError):
        cli.pull(API, prefix, tmp_path / 'download')
    assert not (tmp_path / 'download').exists()


def sbi_batches(site='sbi'):
    records = batches()
    records[0].update(source='brisk_live', trading_date='20260311',
                      input_transport={**schema.BROKER_TRANSPORTS[site], 'decoder': '/private/decoder.js', 'session': 'secret'})
    records[1]['replay_lateness_ms'] = None
    records[-1]['frames'] = 5  # Includes frames used to catch up before bootstrap.
    return records


@pytest.mark.parametrize('site', ['sbi', 'e-shiten'])
def test_broker_market_recording_upload_and_pull(tmp_path, site, online):
    events = tmp_path / 'sbi.jsonl'
    events.write_bytes(stream(sbi_batches(site)).read())
    directory = tmp_path / 'package'
    m = cli.package(events, directory)
    assert m['summary']['source'] == 'brisk_live'
    s3, db = online
    status = stored(s3, db, directory, m)
    assert status['status'] == 'stored'
    cli.pull(API, status['prefix'], tmp_path / 'download')
    assert sorted(kept(s3, status['prefix'])) == sorted([*_parquet.FILES, 'files.json', 'manifest.json'])  # no shared copy
    data = jsonl(tmp_path / 'download') + b''.join(kept(s3, status['prefix']).values())
    assert b'secret' not in data and b'/private/' not in data
    assert json.loads(data.splitlines()[0])['input_transport'] == schema.BROKER_TRANSPORTS[site]
    assert list(cli.manifests(API))[0][1]['summary']['source'] == 'brisk_live'
    # Any broker's transport, its site's provenance, but none that is not a broker's.
    other = sbi_batches(site)
    other[0]['input_transport'] = {'kind': 'sbi_websocket', 'origin': 'https://stream.example/'}
    events.write_bytes(stream(other).read())
    with pytest.raises(ValueError, match='broker transport'):
        cli.package(events, tmp_path / 'mixed')


@pytest.mark.parametrize('source', sorted(schema.LEGACY_BROKER_SOURCES))
def test_a_broker_recording_from_before_one_source_is_refused_with_how_to_upgrade(packaged, online, source):
    directory, m = packaged
    s3, db = online
    code, body = call(s3, db, {'action': 'upload', 'manifest': {**m, 'summary': {**m['summary'], 'source': source}}, 'part': None})
    assert code == 400 and 'upgrade' in body['error'] and 'pip install -U briskapi' in body['error']
    old = {'schema': 'brisk-timing-v2', 'source': source}   # 1.3's: refused before any other check
    code, body = call(s3, db, {'action': 'timing', 'report': old})
    assert code == 400 and 'upgrade' in body['error']


@pytest.mark.parametrize('change', [
    lambda b: b[0].update(cookies={'session': 'secret'}),
    lambda b: b[0]['input_transport'].update(token='secret'),
    lambda b: b[0]['quotes'][0].update(auction_reference_price10=0),   # renamed reference_price10
    lambda b: b[1]['quotes'][0].update(bid_levels=[[1000, 1, 1], [1010, 1, 1]]),   # bids best (highest) first
    lambda b: b[1]['quotes'][0].update(ask_levels=[[1000, 1, 1]] * 2),
    lambda b: b[1]['quotes'][0].update(ask_levels=[[1000 + k, 1, 1] for k in range(11)]),
    lambda b: b[1]['quotes'][0].update(bid_levels=[[1000, 1]]),
    lambda b: b[1].update(skipped_issues=[0]),     # also quoted in the same batch
    lambda b: b[1].update(skipped_issues=[]),
    lambda b: b[1].update(skipped_issues=[5]),     # not in the master
    lambda b: b[-1].update(skipped_issues=[0]),
    lambda b: b[1]['quotes'][0].update(token='secret'),
    lambda b: b[1]['quotes'][0].pop('volume'),
    lambda b: b[1].update(replay_lateness_ms=0),
    lambda b: b[-1].update(frames=0),
    lambda b: b[-1].update(quote_updates=0),
    # Trades: exact fields, identity, bounds, and they account for every volume/turnover change.
    lambda b: b[1]['trades'][0].update(quantity=50),
    lambda b: b[1]['trades'][0].update(price10=1250),
    lambda b: b[1]['quotes'][0].update(volume=200),
    lambda b: b[1]['trades'][0].update(code='1111'),
    lambda b: b[1]['trades'][0].update(time_us=111),
    lambda b: b[1]['trades'][0].update(token='secret'),
    lambda b: b[1]['trades'][0].pop('side'),
    lambda b: b[1]['trades'][0].update(side=256),
    lambda b: b[1]['trades'].append(dict(b[1]['trades'][0], quantity=0)),
    lambda b: b[1]['trades'].append(dict(b[1]['trades'][0], time_us=104)),
    lambda b: b[1]['trades'].append(dict(b[1]['trades'][0], frame=1)),
    lambda b: b[0].pop('trades'),
    lambda b: b[-1].update(trades=[]),
    lambda b: b[1]['quotes'][0].update(bid_closing_levels=[[1000, 1, 1], [1010, 1, 1]]),
    lambda b: b[1]['quotes'][0].update(ask_closing_levels=[[1000, 1]]),
])
def test_sbi_market_recording_rejects_unknown_fields_and_bad_continuity(change):
    records = sbi_batches()
    records[0]['input_transport'] = dict(schema.BROKER_TRANSPORTS['sbi'])
    change(records)
    with pytest.raises(ValueError):
        schema.validate_stream(stream(records))


def test_restored_trades_follow_the_bootstrap():
    # Trades a snapshot restored come in history batches right after the bootstrap, at its clock; the
    # bootstrap's volumes already count them, so they don't enter the volume/turnover check.
    records = sbi_batches()
    records[0]['input_transport'] = dict(schema.BROKER_TRANSPORTS['sbi'])
    old = dict(issue_id=0, code='0000', frame=1, time_us=50, price10=1200, quantity=300, side=2)
    history = [dict(type='history', seq=1, source_time_us=100, trades=[old, dict(old, time_us=60)]),
               dict(type='history', seq=2, source_time_us=100, trades=[dict(old, time_us=70)])]
    shifted = [records[0], *history, *[dict(b, seq=b['seq'] + 2) for b in records[1:]]]
    shifted[-1]['frames'] += 2
    summary = schema.validate_stream(stream(shifted))
    assert summary['batches'] == 5 and summary['quote_updates'] == 2
    directory = Path(__import__('tempfile').mkdtemp())
    _parquet.write(shifted, directory / 'rec')
    assert list(Recording(directory / 'rec').batches()) == shifted
    assert [f['trade_count'] for f in Recording(directory / 'rec').frames()] == [0, 2, 1, 1, 0]
    for change in (lambda b: b.insert(3, dict(b[1], seq=3)),                    # after a quote batch
                   lambda b: b[1].update(source_time_us=101),                    # not at the bootstrap's clock
                   lambda b: b[1].update(trades=[]),
                   lambda b: b[1].update(skipped_issues=[0]),
                   lambda b: b[1].update(trades=[dict(old, time_us=60), old]),   # out of order
                   lambda b: b[1]['trades'][0].update(time_us=101)):             # after the bootstrap
        bad = copy.deepcopy(shifted); change(bad)
        for k, x in enumerate(bad):
            x['seq'] = k
        with pytest.raises(ValueError):
            schema.validate_stream(stream(bad))


def test_books_and_skipped_issues_validate():
    # An issue the decoder could not read at the start is listed in place of its quote, and quoted
    # once it updates; a book side holds up to ten levels, best price first.
    records = sbi_batches()
    records[0]['input_transport'] = dict(schema.BROKER_TRANSPORTS['sbi'])
    records[0]['skipped_issues'] = [records[0]['quotes'].pop()['issue_id']]
    records[1]['quotes'][0].update(bid_levels=[[1230, 300, 2], [1220, 100, 1]], ask_levels=[[1240 + k, 100, 1] for k in range(10)])
    assert schema.validate_stream(stream(records))['quote_updates'] == 1
    records[0].pop('skipped_issues')
    with pytest.raises(ValueError, match='Incomplete bootstrap'):
        schema.validate_stream(stream(records))

class HTTP:
    def __init__(self, value=b''): self.value = value
    def __enter__(self): return self
    def __exit__(self, *a): pass
    def read(self, *a): return self.value

@pytest.mark.parametrize('status', ['stored', 'rejected', 'timeout'])
def test_upload_auto_poll(packaged, monkeypatch, status):
    directory, m = packaged
    responses = [{'ticket': str(uuid.uuid4()), 'upload': {'url': 'https://example.test', 'fields': {'key': 'k'}}},
                 {'status': 'awaiting_upload'}, {'status': status}]
    sent = []
    monkeypatch.setattr(cli, 'request_json', lambda url, data=None, secret=None: sent.append((data, secret)) or responses.pop(0))
    monkeypatch.setattr(cli.urllib.request, 'urlopen', lambda *a, **k: HTTP())
    monkeypatch.setattr(cli.time, 'sleep', lambda *a: None)
    if status == 'timeout':
        monkeypatch.setattr(cli.time, 'monotonic', iter([0, 999]).__next__)
        with pytest.raises(TimeoutError): cli.upload(directory, 'https://api.test', 1)
    elif status == 'rejected':
        with pytest.raises(ValueError): cli.upload(directory, 'https://api.test')
    else:
        assert cli.upload(directory, 'https://api.test', verify=False)['status'] == 'stored'
    # The ticket request names its owner; status polls (by ticket) don't. A replay offers its timing alone.
    assert sent[0] == ({'action': 'upload', 'manifest': m, 'part': part_of(directory)[1]}, SECRET)
    assert all(polled == (None, None) for polled in sent[1:])
    (directory / _parquet.BATCHES).write_bytes(b'changed')
    with pytest.raises(ValueError, match='mismatch'):
        cli.upload(directory, 'https://api.test', verify=False)

@pytest.mark.parametrize('market', [None, True])
def test_upload_sends_the_whole_replay_until_the_archive_holds_its_market_data(packaged, monkeypatch, market):
    directory, m = packaged
    asked, bodies = [], []
    def request_json(url, data=None, secret=None):
        if data is None:
            return {'status': 'stored'}
        asked.append(data['part'])
        if data['part']:
            raise urllib.error.HTTPError(url, 409, 'no copy yet', {}, None)
        return {'ticket': str(uuid.uuid4()), 'upload': {'url': 'https://example.test', 'fields': {'key': 'k'}}}
    monkeypatch.setattr(cli, 'request_json', request_json)
    monkeypatch.setattr(cli.urllib.request, 'urlopen', lambda request, **k: bodies.append(request.data.read()) or HTTP())
    assert cli.upload(directory, API, verify=False, market=market)['status'] == 'stored'
    assert asked == ([None] if market else [part_of(directory)[1], None]) and _parquet.pack(directory) in bodies[0]
    def refuse(url, data=None, secret=None):
        raise urllib.error.HTTPError(url, 503, 'down', {}, None)
    monkeypatch.setattr(cli, 'request_json', refuse)
    with pytest.raises(urllib.error.HTTPError):
        cli.upload(directory, API, verify=False)

def test_https_verifies_against_the_systems_certificates_where_python_has_none(monkeypatch, tmp_path):
    # As Python from python.org on macOS before its "Install Certificates" step: no CA certificate loaded.
    from briskapi import _https
    loaded = []
    class Context:
        def __init__(self, cas): self.cas = cas
        def cert_store_stats(self): return {'x509_ca': self.cas}
        def load_verify_locations(self, bundle): loaded.append(bundle)
    bundle = tmp_path / 'cert.pem'
    bundle.write_text('')
    monkeypatch.setattr(_https, 'SYSTEM_BUNDLES', (str(tmp_path / 'missing.pem'), str(bundle)))
    for cas, expected in ((0, [str(bundle)]), (128, [])):
        _https.context.cache_clear()
        loaded.clear()
        monkeypatch.setattr(_https.ssl, 'create_default_context', lambda cas=cas: Context(cas))
        assert _https.context().cas == cas and loaded == expected
    _https.context.cache_clear()


def test_request_json(monkeypatch):
    seen, contexts = [], []
    monkeypatch.setattr(cli.urllib.request, 'urlopen',
                        lambda request, **kw: seen.append(request) or contexts.append(kw.get('context')) or HTTP(b'{"ok":true}'))
    assert cli.request_json('https://example.test', {'a': 1}) == {'ok': True}
    assert cli.request_json('https://example.test', {'a': 1}, SECRET) == {'ok': True}
    assert seen[0].get_header('Authorization') is None and seen[1].get_header('Authorization') == f'Bearer {SECRET}'
    assert contexts[0] is contexts[1] is cli._https.context(), 'verified with the shared TLS context'
    # Never copied to wherever a redirect points, and never sent over plain HTTP but to this computer.
    assert 'Authorization' not in seen[1].headers and seen[1].unredirected_hdrs['Authorization'] == f'Bearer {SECRET}'
    for url in ('http://example.test', 'ftp://example.test', 'file:///tmp/x'):
        with pytest.raises(ValueError, match='Refusing to send your identity'):
            cli.request_json(url, {'a': 1}, SECRET)
    assert len(seen) == 2 and cli.request_json('http://example.test/?ticket=x') == {'ok': True}
    assert cli.request_json('http://localhost:9000', {'a': 1}, SECRET) == {'ok': True}

def test_package_failure_leaves_nothing(tmp_path):
    events = tmp_path / 'partial.jsonl'
    events.write_bytes(raw(batches()[:-1]))
    with pytest.raises(ValueError, match='clean end'):
        cli.package(events, tmp_path / 'out')
    assert not [p for p in (tmp_path / 'out').rglob('*') if p.is_file()]

def test_package_over_the_archive_limit_leaves_nothing(tmp_path, monkeypatch):
    events = tmp_path / 'source.jsonl'
    events.write_bytes(raw(batches()))
    monkeypatch.setattr(cli, 'max_package', lambda source: 1000)
    with pytest.raises(ValueError, match='accepts up to'):
        cli.package(events, tmp_path / 'out')
    assert not [p for p in (tmp_path / 'out').rglob('*') if p.is_file()]

@pytest.fixture
def harness(tmp_path, monkeypatch):
    config = tmp_path / 'config.json'
    config.write_text(json.dumps(dict(bucket='b', region='ap-northeast-1', api_url='https://api.test')))
    uploads = []
    monkeypatch.setattr(cli, 'upload', lambda *a, **k: uploads.append(a) or {'status': 'stored'})
    def run(cmd, check, stdout=None):
        # Node decoder writes to stdout; the optional Rust collector writes --events.
        if stdout:
            stdout.write(raw(batches()))
        else:
            Path(cmd[cmd.index('--events') + 1]).write_bytes(raw(batches()))
    monkeypatch.setattr(cli.subprocess, 'run', run)
    monkeypatch.setattr(cli, 'check_node', lambda node='node': None)
    (tmp_path / 'source.jsonl').write_bytes(raw(batches()))
    def main(*args):
        cli.main(['--config', str(config), *args])
        return uploads
    return main

@pytest.mark.parametrize('command', ['package', 'record', 'upload', 'list', 'timing', 'pull', 'delete'])
def test_cli(command, packaged, tmp_path, monkeypatch, capsys, harness):
    directory, m = packaged
    cli.save_consent(True)
    monkeypatch.setattr(cli, 'manifests', lambda *a: [('archive/p', m)])
    monkeypatch.setattr(cli, 'timing_reports', lambda *a: [('timing/p', {'frames': 1})])
    monkeypatch.setattr(cli, 'pull', lambda *a: m)
    monkeypatch.setattr(cli, 'delete', lambda *a: {'status': 'deleted'})
    args = [command]
    if command in {'package', 'record'}:
        args += ['--output', str(tmp_path / 'out'), '--upload']
        if command == 'package': args += ['--events', str(tmp_path / 'source.jsonl')]
        else: args += ['--codes', '0000', '--speed', '0']
    elif command == 'upload': args += [str(directory)]
    elif command == 'list': args += ['--source', 'synthetic_test']
    elif command == 'timing': args = ['list', '--timing']
    elif command == 'delete': args += ['archive/p']
    else: args += ['archive/p', '--output', str(tmp_path / 'out')]
    harness(*args)
    assert capsys.readouterr().out

def test_identity(tmp_path, monkeypatch, harness, capsys):
    from briskapi import identity
    monkeypatch.delenv('BRISK_IDENTITY')
    assert identity.secret(create=False) is None and not identity.path().exists()
    # Made on first use, kept owner-only, the same afterwards.
    first = identity.secret()
    assert schema.IDENTITY.fullmatch(first) and identity.secret() == first
    if os.name == 'posix':
        assert identity.path().stat().st_mode & 0o777 == 0o600
    assert json.loads(identity.path().read_text())['owner'] == schema.owner(first) == identity.owner_id()
    harness('identity')
    assert json.loads(capsys.readouterr().out) == {'owner': schema.owner(first), 'kept_in': [str(identity.path())]}
    harness('identity', '--export')
    assert capsys.readouterr().out.strip() == first
    # The recovery code reads back forgivingly (case, spacing, 0/1/8 for O/I/B) and refuses a typo.
    harness('identity', '--recovery-code')
    code = capsys.readouterr().out.strip()
    assert re.fullmatch(r'([A-Z2-7]{4}-){6}[A-Z2-7]{4}', code) and identity.from_recovery_code(code) == first
    assert identity.from_recovery_code(' ' + code.lower().replace('-', ' ').replace('o', '0') + ' ') == first
    assert identity.from_recovery_code(first) == first
    typo = code[:-1] + ('A' if code[-1] != 'A' else 'B')
    with pytest.raises(ValueError, match='mistyped'):
        identity.from_recovery_code(typo)
    # Another computer's identity: refused over a different one unless replaced.
    with pytest.raises(ValueError, match='another identity'):
        harness('identity', '--recover', identity.recovery_code(SECRET))
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO(identity.recovery_code(SECRET) + '\n'))
    harness('identity', '--recover', '-', '--replace')
    assert json.loads(capsys.readouterr().out)['owner'] == OWNER and identity.secret() == SECRET
    with pytest.raises(ValueError, match='Invalid identity'):
        identity.save('brisk-id-v1-nope')
    monkeypatch.setenv('BRISK_IDENTITY', OTHER)
    assert identity.owner_id() == schema.owner(OTHER)
    monkeypatch.delenv('BRISK_IDENTITY')
    identity.path().write_text('garbage')
    with pytest.raises(ValueError, match='Unreadable identity'):
        identity.secret()
    # A copy elsewhere (here: a second store) repairs the file, keeping the unreadable one aside.
    class Store:
        description = 'store'
        def load(self): return {'secret': SECRET}
        def save(self, record): pass
    monkeypatch.setattr(identity, 'copies', lambda: [identity.SystemCopy(Store()), identity.FileCopy()])
    assert identity.secret() == SECRET and identity.FileCopy().load() == SECRET
    assert identity.path().with_name('identity.json.unreadable').read_text() == 'garbage'
    assert identity.where() == ['store', str(identity.path())]

def test_record_without_a_terminal_uploads_by_default(tmp_path, harness, capsys):
    assert len(harness('record', '--cache', str(tmp_path), '--output', str(tmp_path / 'first'))) == 1
    err = capsys.readouterr().err
    assert (tmp_path / 'first/manifest.json').exists() and 'on by default' in err and 'Only you can' in err
    assert f'owner {OWNER}' in err and cli.load_consent() is None
    # Revoked: recordings stay local.
    harness('consent', '--revoke')
    assert len(harness('record', '--cache', str(tmp_path), '--output', str(tmp_path / 'local'))) == 1
    assert (tmp_path / 'local' / _parquet.DEPTHS).exists() and not (tmp_path / 'local/manifest.json').exists()
    assert 'not uploaded' in capsys.readouterr().err
    with pytest.raises(ValueError, match='already exists'):
        harness('record', '--cache', str(tmp_path), '--output', str(tmp_path / 'local'))
    # Packaging is local; only uploading needs consent.
    assert len(harness('package', '--events', str(tmp_path / 'source.jsonl'), '--output', str(tmp_path / 'p'))) == 1
    assert (tmp_path / 'p/manifest.json').exists()
    with pytest.raises(ValueError, match='consent'):
        harness('record', '--cache', str(tmp_path), '--output', str(tmp_path / 'x'), '--upload')

@pytest.mark.parametrize('answer,uploaded', [('\n', True), ('y\n', True), ('n\n', False), ('', False)])
def test_first_run_prompt(tmp_path, monkeypatch, harness, capsys, answer, uploaded):
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO(answer))
    uploads = harness('record', '--output', str(tmp_path / 'r'))
    out = capsys.readouterr()
    assert 'Only you can' in out.err and 'Upload automatically to your private archive?' in out.err
    assert 'Upload automatically' not in out.out
    assert bool(uploads) == uploaded and (tmp_path / 'r/manifest.json').exists() == uploaded
    choice = cli.load_consent()
    assert choice['enabled'] == uploaded and choice['policy_version'] == cli.POLICY_VERSION
    assert set(choice) == {'policy_version', 'enabled', 'decided_at'}
    # The question is asked once; later runs follow the saved choice silently.
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO(''))
    assert bool(harness('record', '--output', str(tmp_path / 'again'))) == uploaded

def test_consent_controls(tmp_path, monkeypatch, harness, capsys):
    harness('consent')
    assert json.loads(capsys.readouterr().out) == {'policy_version': cli.POLICY_VERSION, 'enabled': True, 'default': True}
    harness('consent', '--accept')
    assert json.loads(capsys.readouterr().out)['enabled'] is True
    assert harness('record', '--output', str(tmp_path / 'a')) and (tmp_path / 'a/manifest.json').exists()
    assert len(harness('record', '--no-upload', '--output', str(tmp_path / 'b'))) == 1
    assert (tmp_path / 'b/manifest.json').exists()
    monkeypatch.setenv('BRISK_UPLOAD', '0')
    assert len(harness('package', '--events', str(tmp_path / 'source.jsonl'), '--output', str(tmp_path / 'c'))) == 1
    monkeypatch.delenv('BRISK_UPLOAD')
    assert len(harness('record', '--limit-frames', '2', '--output', str(tmp_path / 'd'))) == 1
    assert (tmp_path / 'd' / _parquet.DEPTHS).exists()
    with pytest.raises(ValueError, match='complete'):
        harness('record', '--limit-frames', '2', '--upload', '--output', str(tmp_path / 'e'))
    harness('consent', '--revoke')
    assert json.loads(capsys.readouterr().out.splitlines()[-1])['enabled'] is False
    assert len(harness('record', '--output', str(tmp_path / 'f'))) == 1
    assert cli.consent_path().name == 'uploads.json'
    cli.consent_path().write_text('{"policy_version": 0, "enabled": true}')
    assert cli.load_consent() is None
    cli.consent_path().write_text('garbage')
    assert cli.load_consent() is None

def test_record_events_command(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(cli.subprocess, 'run', lambda cmd, check, stdout=None: calls.append((cmd, stdout)))
    monkeypatch.setattr(cli, 'check_node', lambda node='node': None)
    cli.record_events(tmp_path / 'e.jsonl', cache=tmp_path, codes=['7203', '6758'], limit_frames=5, speed=0)
    cmd, stdout = calls[0]
    assert cmd[:2] == ['node', str(cli.DECODER)] and stdout is not None and cli.DECODER.exists()
    assert cmd[cmd.index('--codes') + 1] == '7203,6758' and '--limit-frames' in cmd and '--cache' in cmd
    cli.record_events(tmp_path / 'r.jsonl', binary=tmp_path / 'collector')
    cmd, stdout = calls[1]
    assert cmd[0] == str(tmp_path / 'collector') and cmd[cmd.index('--decoder') + 1] == str(cli.DECODER)
    assert '--web' in cmd and stdout is None


def test_lambda_package_is_self_contained(tmp_path):
    """The deployed zip imports the service and loads references without the client API."""
    import importlib.util
    import os
    import subprocess
    import sys
    import zipfile
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('deploy', root / 'infra/deploy.py')
    deploy = importlib.util.module_from_spec(spec); spec.loader.exec_module(deploy)
    with zipfile.ZipFile(io.BytesIO(deploy.package())) as z:
        assert sorted(z.namelist()) == ['archive_service.py', 'briskapi/__init__.py', 'briskapi/_native.py', 'briskapi/_parquet.py',
                                        'briskapi/records.py', 'briskapi/references/historical_mock.json', 'briskapi/schema.py',
                                        'query_engine.py', 'query_service.py']
    # The function that checks uploads also holds brisk-parquet, executable.
    with zipfile.ZipFile(io.BytesIO(deploy.package(b'\x7fELF'))) as z:
        entry = z.getinfo('briskapi/_bin/brisk-parquet')
        assert entry.external_attr >> 16 == 0o100755 and z.read(entry) == b'\x7fELF'
        z.extractall(tmp_path)
    check = ("import archive_service, query_service, briskapi, briskapi.schema as s, sys; "
             f"assert briskapi.__file__.startswith({str(tmp_path)!r}); "
             "assert sorted(s.REFERENCES) == ['historical_mock', 'synthetic_test']; "
             "assert 'briskapi.cli' not in sys.modules")
    subprocess.run([sys.executable, '-c', check], cwd=tmp_path, check=True,
                   env={'PYTHONPATH': str(tmp_path), 'PATH': os.environ.get('PATH', '')})


def test_the_function_runs_a_static_arm64_build_of_brisk_parquet(monkeypatch):
    import importlib.util
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('deploy', root / 'infra/deploy.py')
    deploy = importlib.util.module_from_spec(spec); spec.loader.exec_module(deploy)
    calls = []

    def build(cmd, check):
        calls.append(cmd)
        target = Path(cmd[cmd.index('-v', cmd.index('-v') + 1) + 1].rsplit(':/target', 1)[0]) / 'release'   # (a Windows path has a colon)
        target.mkdir()
        (target / 'brisk-parquet').write_bytes(b'built')
    monkeypatch.setattr(deploy.subprocess, 'run', build)
    assert deploy.native() == b'built'
    command = calls[0]
    assert command[:2] == ['docker', 'run'] and 'linux/arm64' in command and deploy.NATIVE_IMAGE in command
    assert command[-4:] == ['cargo', 'build', '--release', '--locked']


def test_lambda_layer_holds_the_pinned_wheels(monkeypatch):
    import importlib.util
    import zipfile
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('deploy', root / 'infra/deploy.py')
    deploy = importlib.util.module_from_spec(spec); spec.loader.exec_module(deploy)
    calls = []

    def download(cmd, check):
        calls.append(cmd)
        dest = Path(cmd[cmd.index('--dest') + 1])
        with zipfile.ZipFile(dest / 'pyarrow-26.0.0-cp312-cp312-manylinux_2_28_aarch64.whl', 'w') as w:
            w.writestr('pyarrow/__init__.py', '')
            w.writestr('pyarrow/include/arrow/api.h', '')
            w.writestr('pyarrow/tests/test_x.py', '')
    monkeypatch.setattr(deploy.subprocess, 'run', download)
    with zipfile.ZipFile(io.BytesIO(deploy.layer())) as z:
        assert z.namelist() == ['python/pyarrow/__init__.py']
    assert all(r in calls[0] for r in deploy.LAYER_REQUIREMENTS) and 'manylinux_2_28_aarch64' in calls[0]


def test_export(packaged, tmp_path, capsys):
    directory, m = packaged
    cli.main(['export', str(directory), '--jsonl', str(tmp_path / 'events.jsonl')])
    assert (tmp_path / 'events.jsonl').read_bytes() == canon(batches())
    cli.main(['export', str(directory), '--nautilus', str(tmp_path / 'catalog')])
    assert json.loads(capsys.readouterr().out.splitlines()[-1])['files'] == 4
    depths = list((tmp_path / 'catalog/data/order_book_depths/0000.XTKS').glob('*.parquet'))
    table = pq.read_table(depths[0])
    assert table.schema.names == list(_parquet.V2_DEPTH) and table.schema.metadata[b'instrument_id'] == b'0000.XTKS'
    equity = pq.read_table(next((tmp_path / 'catalog/data/equity/0000.XTKS').glob('*.parquet')))
    assert equity.schema.field('info').type == pa.json_(pa.string()) and equity.column('currency')[0].as_py() == 'JPY'
    custom = pq.read_table(next((tmp_path / 'catalog/data/custom/BriskQuote/0000.XTKS').glob('*.parquet')))
    assert custom.column('indicative_price10').to_pylist() == [1230, 1240] and custom.schema.metadata[b'type_name'] == b'BriskQuote'
    assert custom.column('closing_bids').to_pylist() == ['[]', '[]']
    trades = pq.read_table(next((tmp_path / 'catalog/data/trades/0000.XTKS').glob('*.parquet')))
    assert trades.schema.names == list(_parquet.V2_TRADE) and trades.schema.metadata[b'type_name'] == b'TradeTick'
    assert trades.column('aggressor_side').to_pylist() == ['BUY'] and trades.column('trade_id').to_pylist() == ['2-0']
    with pytest.raises(ValueError, match='already exists'):
        cli.export(directory, nautilus=tmp_path / 'catalog')
    with pytest.raises(ValueError, match='already exists'):
        cli.export(directory, jsonl=tmp_path / 'events.jsonl')
    (tmp_path / 'old').mkdir(); (tmp_path / 'old/events.jsonl').write_bytes(canon(batches()))
    with pytest.raises(ValueError, match='Parquet'):
        cli.export(tmp_path / 'old', nautilus=tmp_path / 'other')

def test_an_upload_form_goes_to_the_accelerated_endpoint_when_the_bucket_has_it(monkeypatch):
    # Signed here, offline: the form takes the upload through AWS's nearest edge location (S3 Transfer Acceleration).
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'AKIDEXAMPLE')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'example')
    monkeypatch.setattr(service, 'ACCELERATED', True)
    post = service.poster(None).generate_presigned_post(Bucket='brisk-test-bucket', Key='incoming/t/recording.zip')
    assert post['url'] == 'https://brisk-test-bucket.s3-accelerate.amazonaws.com/'
    monkeypatch.setattr(service, 'ACCELERATED', False)
    s3 = S3()
    assert service.poster(s3) is s3


def test_a_choice_that_lapsed_with_a_new_policy_version_is_named_wherever_it_is_asked_or_defaulted(harness, capsys):
    # Neither a refusal nor an acceptance carries over: each lapses, and says so, in the Python API, in a run that
    # cannot ask, and in the command line.
    import briskapi
    cli.consent_path().parent.mkdir(parents=True, exist_ok=True)
    for enabled, said in ((False, 'not upload'), (True, 'upload')):
        cli.consent_path().write_text(json.dumps({'policy_version': cli.POLICY_VERSION - 1, 'enabled': enabled}))
        assert cli.load_consent() is None and cli.lapsed_consent()['enabled'] is enabled
        with pytest.warns(UserWarning, match=rf'policy has changed \(version {cli.POLICY_VERSION - 1}, now {cli.POLICY_VERSION}\).*choice to {said} no longer'):
            assert briskapi.consent() == {'policy_version': cli.POLICY_VERSION, 'enabled': True, 'default': True}
        harness('consent')
        assert 'policy has changed' in capsys.readouterr().err
        cli.tell_default(__import__("briskapi.identity", fromlist=["new"]).new(), False, cli.lapsed_consent())
        assert f'choice to {said} no longer applies' in capsys.readouterr().err
    cli.consent_path().write_text(json.dumps({'policy_version': cli.POLICY_VERSION, 'enabled': False}))
    assert cli.lapsed_consent() is None and cli.policy_changed(None) == ''   # a current choice is not announced
    cli.consent_path().write_text('garbage')
    assert cli.lapsed_consent() is None
