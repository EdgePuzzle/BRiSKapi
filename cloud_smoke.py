"""Opt-in smoke test of the real private archive (synthetic data, throwaway identities).

Uploads the fixed synthetic probe under a fresh identity, lists and pulls it, checks that another
identity sees none of it and that tampered content is refused, that nothing in the bucket but the
public samples is readable or listable without credentials, then deletes the upload from the owner's
archive (restorable for 30 days).
"""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import urllib.error
import urllib.request

from briskapi import _parquet, identity
from briskapi._historical import Historical
from briskapi._recording import Recording
from briskapi.schema import encode, synthetic_recording
from briskapi.cli import package, upload, manifests, pull, delete, settings


def as_identity(secret):
    os.environ['BRISK_IDENTITY'] = secret


def main():
    config = settings()
    api = config['api_url']
    owner, stranger = identity.new(), identity.new()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        events = root / 'events.jsonl'
        events.write_bytes(b''.join(map(encode, synthetic_recording())))
        m = package(events, root / 'package')
        as_identity(owner)
        result = upload(root / 'package', api)  # the timing table alone, once the archive holds the probe's market data
        prefix = result['prefix']
        assert [p for p, _ in manifests(api, '20210927')] == [prefix]
        pull(api, prefix, root / 'download')
        assert list(Recording(root / 'download').batches()) == synthetic_recording()
        # Another identity neither lists nor downloads it.
        as_identity(stranger)
        assert list(manifests(api)) == []
        try:
            pull(api, prefix, root / 'stranger')
        except urllib.error.HTTPError as e:
            assert e.code == 400
        else:
            raise AssertionError("Another identity downloaded the owner's recording")
        # Content that is not the reference replay must be rejected by the service,
        # even with a self-consistent manifest from a modified client.
        tampered = synthetic_recording(); tampered[1]['quotes'][0]['indicative_price10'] += 10
        _parquet.write(tampered, root / 'tampered')
        data = _parquet.pack(root / 'tampered')
        (root / 'tampered/manifest.json').write_text(json.dumps({**m, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}))
        try:
            upload(root / 'tampered', api, verify=False, market=True)  # the whole recording, not just its timing
        except ValueError as e:
            assert 'rejected' in str(e)
        else:
            raise AssertionError('Service stored non-reference content')
        # Nothing in the bucket is readable or listable without credentials, except the samples.
        base = f"https://{config['bucket']}.s3.{config['region']}.amazonaws.com"
        with urllib.request.urlopen(f'{base}/?list-type=2&prefix=samples/', timeout=30) as response:
            assert response.status == 200
        for url in (f'{base}/{prefix}/manifest.json', f'{base}/{prefix}/recording.zip', f'{base}/?list-type=2&prefix=archive/',
                    f'{base}/?list-type=2&prefix=incoming/'):
            try:
                urllib.request.urlopen(url, timeout=30)
            except urllib.error.HTTPError as e:
                assert e.code == 403, url
            else:
                raise AssertionError(f'Anonymous access allowed: {url}')
        as_identity(owner)
        assert delete(api, prefix)['status'] == 'deleted' and list(manifests(api)) == []
        try:
            pull(api, prefix, root / 'deleted')
        except urllib.error.HTTPError as e:
            assert e.code == 404
        else:
            raise AssertionError('A deleted recording is still downloadable')
        # The cloud API answers with an identity as its key and refuses requests without one.
        query = 'not deployed'
        if config.get('query_url'):
            client = Historical(key=owner, gateway=config['query_url'])
            assert client.metadata.list_datasets() == ['XTKS.BRISK', 'XTKS.BRISKDEMO']
            assert client.metadata.list_publishers()[0]['venue'] == 'XTKS'
            try:
                urllib.request.urlopen(config['query_url'].rstrip('/') + '/v0/metadata.list_datasets', timeout=30)
            except urllib.error.HTTPError as e:
                assert e.code == 400
            else:
                raise AssertionError('The query API answered without a key')
            query = 'passed'
        print(json.dumps({'owner_upload': 'passed', 'owner_pull': 'passed', 'other_identity_denied': 'passed',
                          'tampered_rejected': 'passed', 'anonymous_denied': 'passed', 'deleted': 'passed', 'query_api': query,
                          'region': config['region'], 'prefix': prefix}))


if __name__ == '__main__':
    main()
