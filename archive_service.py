"""Tokyo serverless private archive: an owner-scoped API and automatic S3 ingest.

Every request but a ticket's status names its owner with `Authorization: Bearer <identity secret>`;
the owner ID (a one-way hash of the secret, briskapi.schema.owner) scopes everything it uploads,
lists, downloads or deletes. Only the samples under samples/ are public.

A recording is stored as its four tables under archive/OWNER/YYYYMMDD/FROM-TO-SHA256/, listed in its
files.json, with manifest.json written last as the commit marker. A demo (or synthetic) replay's market
tables are the same for everyone, so the archive keeps them once, under reference/SOURCE/TIMELINE/, and
each such recording stores only its own timing table; such uploads send only that table once the
reference copy exists.

Deleting leaves an owner's objects as noncurrent versions in the versioned bucket: the owner can restore
them for RESTORE_DAYS days, and the bucket's lifecycle removes them a day after that (see PRIVACY.md).
"""
import base64
import datetime as dt
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import tempfile
import time
from urllib.parse import parse_qs
import uuid
import zipfile
import zlib

import boto3
from botocore.exceptions import ClientError
import pyarrow as pa
from briskapi._parquet import BATCHES, FILES, MARKET, digest, pack, unpack, verify_directory
from briskapi.schema import (OWNER, RECORDING_PREFIX, REFERENCES, TIMING_KEY, encode, owner, recording_name, validate_manifest,
                             validate_timing, require, MAX_COMPRESSED, MAX_TIMING_BYTES)

BUCKET = os.environ.get('ARCHIVE_BUCKET', '')
TABLE = os.environ.get('QUOTA_TABLE', '')
# Per-deployment key: stored quota IDs cannot be reversed to IP addresses.
QUOTA_KEY = os.environ.get('QUOTA_SALT', '').encode()
FINAL = {'stored', 'rejected'}
PAGE = {'recordings': 50, 'timing': 500, 'deleted': 200}   # manifests carry their codes, so fewer per page
RESTORE_DAYS = 30
URL_SECONDS = 900
# Malformed input of any kind: corrupt zips and Parquet files and deeply nested JSON included.
INVALID = (ValueError, KeyError, TypeError, IndexError, NotImplementedError, EOFError, OSError, zlib.error,
           RecursionError, zipfile.BadZipFile, pa.ArrowException)

def clients():
    from botocore.config import Config
    region = os.environ.get('AWS_REGION', 'ap-northeast-1')
    return boto3.client('s3', region_name=region, endpoint_url=f'https://s3.{region}.amazonaws.com',
                        config=Config(signature_version='s3v4')), boto3.client('dynamodb', region_name=region)

def json_put(s3, key, data, immutable=False):
    args = dict(Bucket=BUCKET, Key=key, Body=json.dumps(data, sort_keys=True).encode(), ContentType='application/json')
    if immutable:
        args['IfNoneMatch'] = '*'
    try:
        s3.put_object(**args)
    except ClientError as e:
        if not immutable or e.response['Error']['Code'] not in {'PreconditionFailed', 'ConditionalRequestConflict'}:
            raise

def json_get(s3, key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    with obj['Body'] as body:
        raw = body.read(65537)
    require(len(raw) <= 65536, 'Metadata too large')
    return json.loads(raw)

def discard(s3, key, version):
    """Permanently remove one staging object version; already-removed versions are fine."""
    try:
        s3.delete_object(Bucket=BUCKET, Key=key, VersionId=version)
    except ClientError as e:
        if e.response['Error']['Code'] not in {'NoSuchKey', 'NoSuchVersion'}:
            raise

def quota(db, scope, amount, limit, window):
    now = int(time.time())
    try:
        db.update_item(TableName=TABLE, Key={'id': {'S': f'{scope}:{now // window}'}},
            UpdateExpression='SET expires = :expires ADD used :amount',
            ConditionExpression='attribute_not_exists(used) OR used <= :remaining',
            ExpressionAttributeValues={':expires': {'N': str(now + window * 2)},
                ':amount': {'N': str(amount)}, ':remaining': {'N': str(limit - amount)}})
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            raise ValueError('Upload quota reached; retry later') from e
        raise

class NotFound(Exception):
    pass

def reference_prefix(source):
    """Where the archive keeps a reference replay's market tables, once; None for sources without one."""
    reference = REFERENCES.get(source)
    return reference and f"reference/{source}/{reference['timeline']}"

def _part(value):
    """The timing-only part an upload sends instead of the whole recording, or None."""
    if value is None:
        return None
    require(isinstance(value, dict) and set(value) == {'sha256', 'bytes'} and isinstance(value['sha256'], str)
            and re.fullmatch('[0-9a-f]{64}', value['sha256']) and isinstance(value['bytes'], int)
            and 0 < value['bytes'] <= MAX_COMPRESSED, 'Invalid part')
    return value

def response(status, value):
    return {'statusCode': status, 'headers': {'content-type': 'application/json', 'cache-control': 'no-store'},
            'body': json.dumps(value)}

def api(event, s3, db):
    method = event['requestContext']['http']['method']
    if method == 'GET':
        ticket = parse_qs(event.get('rawQueryString', '')).get('ticket', [''])[0]
        require(str(uuid.UUID(ticket)) == ticket, 'Invalid ticket')
        try:
            return response(200, json_get(s3, f'incoming/{ticket}/status.json'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return response(404, {'error': 'Ticket not found'})
            raise
    require(method == 'POST', 'Use POST')
    raw = event.get('body', '')
    if event.get('isBase64Encoded'):
        raw = base64.b64decode(raw, validate=True).decode()
    require(len(raw) <= 65536, 'Request too large')
    body = json.loads(raw)
    require(isinstance(body, dict) and body.get('action') in ACTIONS and set(body) == ACTIONS[body['action']][0],
            'Unknown action or fields')
    authorization = (event.get('headers') or {}).get('authorization', '')
    require(authorization.startswith('Bearer '), 'An identity is required (Authorization: Bearer <secret>)')
    who = owner(authorization[len('Bearer '):])
    require(QUOTA_KEY, 'Archive service is not configured')
    ip = 'ip-' + hmac.new(QUOTA_KEY, event['requestContext']['http'].get('sourceIp', 'unknown').encode(), hashlib.sha256).hexdigest()
    return ACTIONS[body['action']][1](body, who, ip, s3, db)

def upload(body, who, ip, s3, db):
    # Validate the complete manifest before retaining anything.
    m = validate_manifest(body['manifest'])
    require(len(json.dumps(m['summary'])) < 60000, 'Summary too large')
    part = _part(body['part'])
    if part:
        shared = reference_prefix(m['summary']['source'])
        require(shared, 'Only a demo or synthetic replay can send its timing alone')
        if not _exists(s3, f'{shared}/complete.json'):
            return response(409, {'error': "The archive holds no copy of this replay's market data yet; "
                                           'upload the whole recording', 'send': 'recording'})
    sent = part or m
    quota(db, ip, 1, 4, 3600)
    quota(db, 'owner-' + who, 1, 8, 3600)
    quota(db, 'global-tickets', 1, 64, 3600)
    quota(db, 'global-bytes', sent['bytes'], 5 * 1024**3, 86400)
    ticket = str(uuid.uuid4())
    json_put(s3, f'incoming/{ticket}/manifest.json', m)
    if part:
        json_put(s3, f'incoming/{ticket}/part.json', part)
    json_put(s3, f'incoming/{ticket}/owner.json', {'owner': who})
    json_put(s3, f'incoming/{ticket}/status.json', {'status': 'awaiting_upload', 'ticket': ticket})
    fields = {'Content-Type': 'application/zip', 'x-amz-server-side-encryption': 'AES256'}
    post = s3.generate_presigned_post(Bucket=BUCKET, Key=f'incoming/{ticket}/recording.zip',
        Fields=fields, Conditions=[{'Content-Type': 'application/zip'},
        {'x-amz-server-side-encryption': 'AES256'}, ['content-length-range', sent['bytes'], sent['bytes']]], ExpiresIn=900)
    return response(201, {'ticket': ticket, 'upload': post, 'expires_seconds': 900})

def timing(body, who, ip, s3, db):
    """Keep a timing report (no market data): validated, re-serialized by the service, the owner's only."""
    report = validate_timing(body['report'], today=dt.datetime.now(dt.timezone.utc).date())
    data = encode(report)
    require(len(data) <= MAX_TIMING_BYTES, 'Timing report too large')
    quota(db, 'timing-' + ip, 1, 6, 3600)
    quota(db, 'global-timing', 1, 600, 3600)
    key = f"timing/{who}/{report['trading_date']}/{hashlib.sha256(data).hexdigest()}.json"
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType='application/json', IfNoneMatch='*')
    except ClientError as e:
        if e.response['Error']['Code'] not in {'PreconditionFailed', 'ConditionalRequestConflict'}:
            raise
    return response(201, {'status': 'stored', 'key': key})

def listing(body, who, ip, s3, db):
    """A page of the owner's recordings (prefix and manifest) or timing reports (key and report), or what it
    deleted and can still restore."""
    kind, date, after = body['kind'], body['date'], body['after']
    require(kind in PAGE, 'Unknown listing')
    require(date is None or (isinstance(date, str) and len(date) == 8 and date.isdigit()), 'Date must be YYYYMMDD')
    if kind == 'deleted':
        require(after is None, 'Invalid page')
        return response(200, {'items': _deleted(s3, who, date)[:PAGE['deleted']], 'next': None})
    root = f"{'archive' if kind == 'recordings' else 'timing'}/{who}/"
    prefix = root + (f'{date}/' if date else '')
    require(after is None or (isinstance(after, str) and after.startswith(prefix) and len(after) < 300), 'Invalid page')
    items, last = [], None
    pages = s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=prefix, **({'StartAfter': after} if after else {}))
    for page in pages:
        for item in page.get('Contents', []):
            key = item['Key']
            if kind == 'recordings' and key.endswith('/manifest.json'):
                items.append({'prefix': key.rsplit('/', 1)[0], 'manifest': json_get(s3, key)})
            elif kind == 'timing' and TIMING_KEY.fullmatch(key):
                items.append({'key': key, 'report': json_get(s3, key)})
            else:
                continue
            last = key
            if len(items) == PAGE[kind]:
                return response(200, {'items': items, 'next': last})
    return response(200, {'items': items, 'next': None})

def download(body, who, ip, s3, db):
    """A recording's manifest and a short-lived URL for each of its tables; the owner's only."""
    prefix = _owned(body['prefix'], who)
    try:
        manifest = json_get(s3, f'{prefix}/manifest.json')
        files = json_get(s3, f'{prefix}/files.json')
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise NotFound(prefix) from None
        raise
    shared = reference_prefix(manifest['summary']['source'])
    require(isinstance(files, dict) and set(files) == set(FILES)
            and all(key in {f'{prefix}/{name}', f'{shared}/{name}'} for name, key in files.items()), 'Invalid stored recording')
    urls = {name: s3.generate_presigned_url('get_object', Params={'Bucket': BUCKET, 'Key': key}, ExpiresIn=URL_SECONDS)
            for name, key in files.items()}
    return response(200, {'manifest': manifest, 'files': urls, 'expires_seconds': URL_SECONDS})

def delete(body, who, ip, s3, db):
    """Remove one of the owner's recordings (or timing reports) from its archive.

    The objects' current versions are deleted, so the owner can no longer list or download them; their
    previous versions stay in the versioned bucket, the operator's copy, which this function cannot purge.
    """
    target = body['prefix']
    if isinstance(target, str) and TIMING_KEY.fullmatch(target):
        require(TIMING_KEY.fullmatch(target)[1] == who, 'Not your upload')
        keys = [target] if _exists(s3, target) else []
    else:
        prefix = _owned(target, who) + '/'
        keys = [item['Key'] for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=prefix)
                for item in page.get('Contents', [])]
    if not keys:
        raise NotFound(target)
    # The manifest (the listing's commit marker) goes first, so a listing never shows half a recording.
    for key in sorted(keys, key=lambda k: not k.endswith('manifest.json')):
        s3.delete_object(Bucket=BUCKET, Key=key)
    return response(200, {'status': 'deleted', 'removed': len(keys),
                          'note': f'Removed from your archive; `brisk restore` brings it back within {RESTORE_DAYS} days.'})

def restore(body, who, ip, s3, db):
    """Bring back one of the owner's recordings (or timing reports) deleted in the last RESTORE_DAYS days."""
    target = body['prefix']
    if isinstance(target, str) and TIMING_KEY.fullmatch(target):
        require(TIMING_KEY.fullmatch(target)[1] == who, 'Not your upload')
        kept = {key: version for key, version in _restorable(s3, target).items() if key == target}
    else:
        kept = _restorable(s3, _owned(target, who) + '/')
    if not kept:
        raise NotFound(target)
    # The manifest, the listing's commit marker, comes back last.
    for key in sorted(kept, key=lambda k: k.endswith('manifest.json')):
        s3.copy_object(Bucket=BUCKET, Key=key, CopySource={'Bucket': BUCKET, 'Key': key, 'VersionId': kept[key]})
    return response(200, {'status': 'restored', 'restored': len(kept)})

EARLIEST = dt.datetime.min.replace(tzinfo=dt.timezone.utc)

def versions(s3, prefix):
    """Per key below `prefix`: 'deleted' (when, if its latest version is a delete marker), 'current' (if it
    has a current version) and 'kept' (its newest noncurrent version: id and time)."""
    found = {}
    for page in s3.get_paginator('list_object_versions').paginate(Bucket=BUCKET, Prefix=prefix):
        for marker in page.get('DeleteMarkers', []):
            if marker['IsLatest']:
                found.setdefault(marker['Key'], {})['deleted'] = marker['LastModified']
        for version in page.get('Versions', []):
            entry = found.setdefault(version['Key'], {})
            if version['IsLatest']:
                entry['current'] = True
            elif version['LastModified'] > entry.get('kept', (None, EARLIEST))[1]:
                entry['kept'] = (version['VersionId'], version['LastModified'])
    return found

def _restorable(s3, prefix, now=None):
    """{key: version to restore} of what was deleted below `prefix` within RESTORE_DAYS days."""
    now = now or dt.datetime.now(dt.timezone.utc)
    return {key: entry['kept'][0] for key, entry in versions(s3, prefix).items()
            if 'deleted' in entry and 'kept' in entry and now - entry['deleted'] <= dt.timedelta(days=RESTORE_DAYS)}

def version_json(s3, key, version):
    obj = s3.get_object(Bucket=BUCKET, Key=key, VersionId=version)
    with obj['Body'] as body:
        raw = body.read(65537)
    require(len(raw) <= 65536, 'Metadata too large')
    return json.loads(raw)

def _deleted(s3, who, date):
    """The owner's deleted recordings and timing reports it can still restore, with when they go."""
    now, items = dt.datetime.now(dt.timezone.utc), []
    for root in (f'archive/{who}/', f'timing/{who}/'):
        entries = versions(s3, root + (f'{date}/' if date else ''))
        for key, entry in sorted(entries.items()):
            if not ('deleted' in entry and 'kept' in entry and now - entry['deleted'] <= dt.timedelta(days=RESTORE_DAYS)):
                continue
            when = {'deleted_at': entry['deleted'].isoformat(timespec='seconds'),
                    'restore_until': (entry['deleted'] + dt.timedelta(days=RESTORE_DAYS)).isoformat(timespec='seconds')}
            if key.endswith('/manifest.json') and RECORDING_PREFIX.fullmatch(key.rsplit('/', 1)[0]):
                items.append({'prefix': key.rsplit('/', 1)[0], 'manifest': version_json(s3, key, entry['kept'][0]), **when})
            elif TIMING_KEY.fullmatch(key):
                items.append({'key': key, 'report': version_json(s3, key, entry['kept'][0]), **when})
    return items

def _exists(s3, key):
    try:
        s3.get_object(Bucket=BUCKET, Key=key)['Body'].close()
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return False
        raise

def _owned(prefix, who):
    require(isinstance(prefix, str) and RECORDING_PREFIX.fullmatch(prefix), 'Invalid recording prefix')
    require(RECORDING_PREFIX.fullmatch(prefix)[1] == who, 'Not your upload')
    return prefix

ACTIONS = {
    'upload': ({'action', 'manifest', 'part'}, upload),
    'timing': ({'action', 'report'}, timing),
    'list': ({'action', 'kind', 'date', 'after'}, listing),
    'download': ({'action', 'prefix'}, download),
    'restore': ({'action', 'prefix'}, restore),
    'delete': ({'action', 'prefix'}, delete),
}

def ingest(record, s3, db):
    from urllib.parse import unquote_plus
    require(record['s3']['bucket']['name'] == BUCKET, 'Unexpected bucket')
    obj = record['s3']['object']
    key = unquote_plus(obj['key'])
    parts = key.split('/')
    require(len(parts) == 3 and parts[0] == 'incoming' and parts[2] == 'recording.zip', 'Unexpected key')
    ticket = str(uuid.UUID(parts[1]))
    require(ticket == parts[1], 'Invalid ticket')
    version = obj.get('versionId')
    # Require a version so validation and storage use the same immutable input.
    require(version, 'Versioned upload required')
    try:
        db.update_item(TableName=TABLE, Key={'id': {'S': 'ticket-' + ticket}},
            UpdateExpression='SET version = :version, expires = :expires',
            ConditionExpression='attribute_not_exists(version) OR version = :version',
            ExpressionAttributeValues={':version': {'S': version}, ':expires': {'N': str(int(time.time()) + 172800)}})
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            # A ticket admits only its first object version. Later POSTs with the
            # same upload form never become data and are not kept in staging.
            discard(s3, key, version)
            return
        raise
    if json_get(s3, f'incoming/{ticket}/status.json').get('status') in FINAL:
        discard(s3, key, version)  # Redelivered event for a finished ticket.
        return
    try:
        m = json_get(s3, f'incoming/{ticket}/manifest.json')
        validate_manifest(m)
        who = json_get(s3, f'incoming/{ticket}/owner.json')['owner']
        require(isinstance(who, str) and OWNER.fullmatch(who), 'Invalid owner')
        part = _part(_optional(s3, f'incoming/{ticket}/part.json'))
        sent = part or m
        upload = s3.get_object(Bucket=BUCKET, Key=key, VersionId=version)
        require(upload['ContentLength'] == sent['bytes'] <= MAX_COMPRESSED, 'Invalid upload size')
        with upload['Body'] as body:
            data = body.read(MAX_COMPRESSED + 1)
        require(len(data) == sent['bytes'] and digest(data) == sent['sha256'], 'Size/hash mismatch')
        prefix, stored = store(data, m, part, who, s3)
        json_put(s3, f'incoming/{ticket}/status.json', {'status': 'stored', 'prefix': prefix, 'sha256': stored['sha256']})
    except INVALID as e:
        # No raw payload or potentially sensitive contents in the ticket's status or logs.
        json_put(s3, f'incoming/{ticket}/status.json', {'status': 'rejected', 'reason': type(e).__name__})
    discard(s3, key, version)

def _optional(s3, key):
    try:
        return json_get(s3, key)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        raise

def _fetch(s3, prefix, directory, names):
    for name in names:
        obj = s3.get_object(Bucket=BUCKET, Key=f'{prefix}/{name}')
        with obj['Body'] as body:
            data = body.read(MAX_COMPRESSED + 1)
        require(len(data) <= MAX_COMPRESSED, 'Stored table too large')
        (directory / name).parent.mkdir(parents=True, exist_ok=True)
        (directory / name).write_bytes(data)

def _put(s3, key, data):
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType='application/vnd.apache.parquet', IfNoneMatch='*')
    except ClientError as e:
        if e.response['Error']['Code'] not in {'PreconditionFailed', 'ConditionalRequestConflict'}:
            raise

def store(data, m, part, who, s3):
    """Verify an upload (the whole recording, or a replay's timing table to join the stored market data) and store
    it; returns (prefix, stored manifest). The stored manifest hashes the zip of the four tables as stored."""
    shared = reference_prefix(m['summary']['source'])
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        if part:
            require(_exists(s3, f'{shared}/complete.json'), "No stored copy of this replay's market data")
            unpack(data, directory, (BATCHES,))
            _fetch(s3, shared, directory, MARKET)
        else:
            unpack(data, directory)
        verify_directory(directory, m)
        if shared:
            if not part:
                # The first verified replay becomes the archive's copy (each table is the same for everyone),
                # and every owner's recording uses the copy as stored.
                if not _exists(s3, f'{shared}/complete.json'):
                    for name in MARKET:
                        _put(s3, f'{shared}/{name}', (directory / name).read_bytes())
                    json_put(s3, f'{shared}/complete.json', {'source': m['summary']['source']}, immutable=True)
                _fetch(s3, shared, directory, MARKET)
            own = (BATCHES,)
        else:
            own = FILES
        blob = pack(directory)
        stored = validate_manifest({**m, 'sha256': digest(blob), 'bytes': len(blob)})
        prefix = f'archive/{who}/{recording_name(stored)}'
        for name in own:
            _put(s3, f'{prefix}/{name}', (directory / name).read_bytes())
    json_put(s3, f'{prefix}/files.json', {name: f'{prefix if name in own else shared}/{name}' for name in FILES},
             immutable=True)
    # Manifest is the commit marker. Readers list only committed datasets.
    json_put(s3, f'{prefix}/manifest.json', stored, immutable=True)
    return prefix, stored

def handler(event, context):
    s3, db = clients()
    if 'Records' in event:
        for record in event['Records']:
            ingest(record, s3, db)
        return {'ok': True}
    try:
        return api(event, s3, db)
    except NotFound:
        return response(404, {'error': 'Not found'})
    except INVALID as e:
        return response(400, {'error': str(e)[:160]})
