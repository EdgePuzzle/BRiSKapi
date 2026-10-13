"""Opt-in real automatic-publication/anonymous-download smoke test (synthetic data)."""
import hashlib
import json
from pathlib import Path
import tempfile

from briskapi import _parquet
from briskapi._recording import Recording
from briskapi.schema import encode, synthetic_recording
from briskapi.cli import package, contribute, client, manifests, pull, settings

def main():
    config=settings()
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp)
        # The fixed synthetic probe deduplicates: repeated runs publish one object.
        events=root/'events.jsonl'
        events.write_bytes(b''.join(map(encode,synthetic_recording())))
        m=package(events,root/'package','archive-smoke-test','CC0-1.0')
        result=contribute(root/'package',config['api_url'])
        s3=client(config)
        assert [p for p,_ in manifests(s3,config['bucket'],'20210927') if p==result['prefix']]
        pull(s3,config['bucket'],result['prefix'],root/'download')
        assert list(Recording(root/'download').batches())==synthetic_recording()
        # Content that is not the reference replay must be rejected by the service,
        # even with a self-consistent manifest from a modified client.
        tampered=synthetic_recording(); tampered[1]['quotes'][0]['indicative_price10']+=10
        _parquet.write(tampered,root/'tampered')
        data=_parquet.pack(root/'tampered')
        (root/'tampered/manifest.json').write_text(json.dumps({**m,'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}))
        try:
            contribute(root/'tampered',config['api_url'],verify=False)
        except ValueError as e:
            assert 'rejected' in str(e)
        else:
            raise AssertionError('Service published non-reference content')
        # Public listing and writing to staging must remain forbidden without a ticket.
        from botocore.exceptions import ClientError
        for method,kwargs in [(s3.list_objects_v2,dict(Prefix='incoming/')),
                              (s3.put_object,dict(Key='incoming/forbidden',Body=b'no'))]:
            try:
                method(Bucket=config['bucket'],**kwargs)
            except ClientError as e:
                assert e.response['Error']['Code']=='AccessDenied'
            else:
                raise AssertionError('Anonymous staging access was allowed')
        print(json.dumps({'automatic_publication':'passed','anonymous_verified_pull':'passed',
                          'tampered_rejected':'passed','staging_private':'passed',
                          'region':config['region'],'prefix':result['prefix']}))

if __name__=='__main__':
    main()
