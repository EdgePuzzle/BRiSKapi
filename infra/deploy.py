#!/usr/bin/env python3
"""Provision/update a standalone private S3 + Lambda + DynamoDB archive in Tokyo."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
import zipfile

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
REGION = 'ap-northeast-1'
NAME = 'brisk-recorder-archive'
# Deleted uploads stay restorable for 30 days (archive_service.RESTORE_DAYS); the bucket drops them after this.
DELETED_DAYS = 31
# The service's Parquet dependencies, pinned: a layer of their manylinux wheels for python3.12 on arm64.
LAYER_REQUIREMENTS = ('pyarrow==26.0.0', 'numpy==2.5.3')

def ignore_exists(call, **kwargs):
    try:
        return call(**kwargs)
    except ClientError as e:
        if e.response['Error']['Code'] not in {'EntityAlreadyExists', 'ResourceInUseException', 'ResourceConflictException', 'BucketAlreadyOwnedByYou', 'ResourceAlreadyExistsException'}:
            raise

def package():
    """The Lambda zip: the service, the shared schema and Parquet format, and reference fingerprints only."""
    code=io.BytesIO()
    with zipfile.ZipFile(code,'w',zipfile.ZIP_DEFLATED) as z:
        z.write(ROOT/'archive_service.py','archive_service.py')
        # An empty package init keeps the client API (and its dependencies) out.
        z.writestr('briskapi/__init__.py','')
        z.write(ROOT/'briskapi/schema.py','briskapi/schema.py')
        z.write(ROOT/'briskapi/_parquet.py','briskapi/_parquet.py')
        # Reference fingerprints verify demo/synthetic content; broker recordings are structurally checked.
        for path in sorted((ROOT/'briskapi/references').glob('*.json')):
            z.write(path,f'briskapi/references/{path.name}')
    return code.getvalue()

def layer():
    """The dependency layer zip: LAYER_REQUIREMENTS' wheels unpacked under python/ (needs pip and PyPI)."""
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run([sys.executable,'-m','pip','download','--only-binary=:all:','--no-deps','--implementation','cp',
                        '--python-version','3.12','--platform','manylinux_2_28_aarch64','--platform','manylinux2014_aarch64',
                        '--dest',tmp,*LAYER_REQUIREMENTS],check=True)
        out=io.BytesIO()
        with zipfile.ZipFile(out,'w',zipfile.ZIP_DEFLATED) as z:
            for wheel in sorted(Path(tmp).glob('*.whl')):
                with zipfile.ZipFile(wheel) as w:
                    for info in w.infolist():
                        # C++ headers, Cython sources and test suites are not needed at run time.
                        if info.is_dir() or '/include/' in info.filename or '/tests/' in info.filename \
                                or info.filename.endswith(('.pxd','.pyx','.pxi','.h','.cc')):
                            continue
                        entry=zipfile.ZipInfo(f'python/{info.filename}',date_time=(1980,1,1,0,0,0))
                        entry.compress_type,entry.external_attr=zipfile.ZIP_DEFLATED,info.external_attr
                        z.writestr(entry,w.read(info))
        return out.getvalue()

def publish_layer(lam, s3, bucket):
    """This layer's version ARN, published from the (private) deploy/ prefix unless already published."""
    data=layer()
    tag=hashlib.sha256(data).hexdigest()[:16]
    for version in lam.list_layer_versions(LayerName=NAME+'-deps').get('LayerVersions',[]):
        if version.get('Description')==tag:
            return version['LayerVersionArn']
    # Over 50 MB, so the layer goes through the (private) bucket.
    key=f'deploy/layer-{tag}.zip'
    s3.put_object(Bucket=bucket,Key=key,Body=data)
    return lam.publish_layer_version(LayerName=NAME+'-deps',Description=tag,Content={'S3Bucket':bucket,'S3Key':key},
        CompatibleRuntimes=['python3.12'],CompatibleArchitectures=['arm64'])['LayerVersionArn']

def publish_samples(s3, bucket):
    """Copy the legacy wire-format sample (schema brisk-sbi-wire-jsonl-v1, stored while the archive was public)
    to the public samples/ prefix. Idempotent; the originals stay where they are."""
    copied = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix='archive/'):
        for item in page.get('Contents', []):
            key = item['Key']
            parts = key.split('/')
            # The public archive's layout was archive/YYYYMMDD/SHA256/file; owners' have one more level.
            if len(parts) != 4 or parts[3] != 'manifest.json':
                continue
            manifest = json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read(65537))
            if manifest.get('schema') != 'brisk-sbi-wire-jsonl-v1':
                continue
            source = key.rsplit('/', 1)[0]
            for name in ('events.jsonl.gz', 'manifest.json'):
                target = f"samples/{'/'.join(parts[1:3])}/{name}"
                try:
                    s3.head_object(Bucket=bucket, Key=target)
                except ClientError:
                    s3.copy_object(Bucket=bucket, Key=target, CopySource={'Bucket': bucket, 'Key': f'{source}/{name}'})
                    copied.append(target)
    return copied

def deploy(bucket):
    session = boto3.Session(region_name=REGION)
    s3, iam, lam, db = [session.client(n) for n in ('s3', 'iam', 'lambda', 'dynamodb')]
    account = session.client('sts').get_caller_identity()['Account']
    ignore_exists(s3.create_bucket, Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': REGION})
    location = s3.get_bucket_location(Bucket=bucket)['LocationConstraint']
    if location != REGION:
        raise ValueError('Archive bucket must be in Tokyo')
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={'Status':'Enabled'})
    s3.put_bucket_ownership_controls(Bucket=bucket, OwnershipControls={'Rules':[{'ObjectOwnership':'BucketOwnerEnforced'}]})
    s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={'Rules':[{'ApplyServerSideEncryptionByDefault':{'SSEAlgorithm':'AES256'}}]})
    # Only samples/ (the operator's sample files) is public; each owner reaches its own uploads through the
    # function only.
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
        'BlockPublicAcls':True, 'IgnorePublicAcls':True, 'BlockPublicPolicy':False, 'RestrictPublicBuckets':False})
    arn = f'arn:aws:s3:::{bucket}'
    policy = {'Version':'2012-10-17', 'Statement':[
        {'Sid':'ReadSamples','Effect':'Allow','Principal':'*','Action':'s3:GetObject','Resource':arn+'/samples/*'},
        {'Sid':'ListSamples','Effect':'Allow','Principal':'*','Action':'s3:ListBucket','Resource':arn,
         'Condition':{'StringLike':{'s3:prefix':['samples/','samples/*']}}},
        {'Sid':'TLSOnly','Effect':'Deny','Principal':'*','Action':'s3:*','Resource':[arn,arn+'/*'],
         'Condition':{'Bool':{'aws:SecureTransport':'false'}}}]}
    s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
    s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={'Rules':[
        {'ID':'ExpireStaging','Status':'Enabled','Filter':{'Prefix':'incoming/'},'Expiration':{'Days':2},
         'NoncurrentVersionExpiration':{'NoncurrentDays':1},'AbortIncompleteMultipartUpload':{'DaysAfterInitiation':1}},
        # What an owner deletes can be restored for 30 days (archive_service.RESTORE_DAYS); a day later it is gone.
        *({'ID':f'ExpireDeleted-{prefix[:-1]}','Status':'Enabled','Filter':{'Prefix':prefix},
           'NoncurrentVersionExpiration':{'NoncurrentDays':DELETED_DAYS},'Expiration':{'ExpiredObjectDeleteMarker':True}}
          for prefix in ('archive/', 'timing/'))]})
    ignore_exists(db.create_table, TableName=NAME, AttributeDefinitions=[{'AttributeName':'id','AttributeType':'S'}],
        KeySchema=[{'AttributeName':'id','KeyType':'HASH'}], BillingMode='PAY_PER_REQUEST')
    db.get_waiter('table_exists').wait(TableName=NAME)
    if db.describe_time_to_live(TableName=NAME)['TimeToLiveDescription']['TimeToLiveStatus'] == 'DISABLED':
        db.update_time_to_live(TableName=NAME, TimeToLiveSpecification={'Enabled':True,'AttributeName':'expires'})
    trust={'Version':'2012-10-17','Statement':[{'Effect':'Allow','Principal':{'Service':'lambda.amazonaws.com'},'Action':'sts:AssumeRole'}]}
    role = ignore_exists(iam.create_role, RoleName=NAME, AssumeRolePolicyDocument=json.dumps(trust))
    role_arn = iam.get_role(RoleName=NAME)['Role']['Arn']
    iam.put_role_policy(RoleName=NAME, PolicyName=NAME, PolicyDocument=json.dumps({'Version':'2012-10-17','Statement':[
        {'Effect':'Allow','Action':['s3:GetObject','s3:GetObjectVersion','s3:PutObject'],'Resource':[arn+'/incoming/*',arn+'/archive/*',arn+'/timing/*',arn+'/reference/*']},
        # Owners list their own uploads (the function scopes each listing to the caller's owner ID).
        # Unconditional ListBucket, so that a missing key reads as 404 (NoSuchKey), not 403: the service
        # tests for staging parts, reference copies and tickets that way.
        {'Effect':'Allow','Action':'s3:ListBucket','Resource':arn},
        {'Effect':'Allow','Action':'s3:ListBucketVersions','Resource':arn,
         'Condition':{'StringLike':{'s3:prefix':['archive/*','timing/*']}}},
        # Staging uploads are removed once validated, rejected or superseded.
        {'Effect':'Allow','Action':['s3:DeleteObject','s3:DeleteObjectVersion'],'Resource':arn+'/incoming/*'},
        # Owners remove their own uploads from their archive; the function can't purge versions there, so they stay
        # restorable until the lifecycle expires them (DELETED_DAYS).
        {'Effect':'Allow','Action':'s3:DeleteObject','Resource':[arn+'/archive/*',arn+'/timing/*']},
        {'Effect':'Allow','Action':'dynamodb:UpdateItem','Resource':f'arn:aws:dynamodb:{REGION}:{account}:table/{NAME}'},
        {'Effect':'Allow','Action':['logs:CreateLogStream','logs:PutLogEvents'],'Resource':f'arn:aws:logs:{REGION}:{account}:log-group:/aws/lambda/{NAME}:*'}]}))
    logs=session.client('logs')
    ignore_exists(logs.create_log_group, logGroupName=f'/aws/lambda/{NAME}')
    logs.put_retention_policy(logGroupName=f'/aws/lambda/{NAME}', retentionInDays=14)
    code=io.BytesIO(package())
    try:
        existing=lam.get_function_configuration(FunctionName=NAME)['Environment']['Variables']
    except (lam.exceptions.ResourceNotFoundException, KeyError):
        existing={}
    # Keyed IP hashing for rate limits; keep the key across redeploys.
    salt=existing.get('QUOTA_SALT') or secrets.token_hex(32)
    # Verifying a complete replay holds its decoded columns in memory: about 2.2 GB for the demo.
    config=dict(Runtime='python3.12',Role=role_arn,Handler='archive_service.handler',Timeout=600,MemorySize=4096,
                Layers=[publish_layer(lam,s3,bucket)],
                Environment={'Variables':{'ARCHIVE_BUCKET':bucket,'QUOTA_TABLE':NAME,'QUOTA_SALT':salt}},
                EphemeralStorage={'Size':512})
    try:
        lam.get_function(FunctionName=NAME)
    except lam.exceptions.ResourceNotFoundException:
        for attempt in range(12):
            try:
                lam.create_function(FunctionName=NAME,Code={'ZipFile':code.getvalue()},Architectures=['arm64'],**config)
                break
            except lam.exceptions.InvalidParameterValueException:
                if attempt == 11:
                    raise
                time.sleep(5)
        lam.get_waiter('function_active_v2').wait(FunctionName=NAME)
    else:
        lam.update_function_configuration(FunctionName=NAME,**config)
        lam.get_waiter('function_updated_v2').wait(FunctionName=NAME)
        lam.update_function_code(FunctionName=NAME,ZipFile=code.getvalue(),Architectures=['arm64'])
        lam.get_waiter('function_updated_v2').wait(FunctionName=NAME)
    try:
        lam.put_function_concurrency(FunctionName=NAME,ReservedConcurrentExecutions=4)
    except lam.exceptions.InvalidParameterValueException as e:
        if 'UnreservedConcurrentExecution' not in str(e):
            raise
        print('Regional account quota prevents reserving concurrency; application upload quotas remain active.')
    lam.put_function_event_invoke_config(FunctionName=NAME,MaximumRetryAttempts=2,MaximumEventAgeInSeconds=3600)
    function_arn=lam.get_function(FunctionName=NAME)['Configuration']['FunctionArn']
    ignore_exists(lam.add_permission,FunctionName=NAME,StatementId='S3Ingest',Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',SourceArn=arn,SourceAccount=account)
    # Current function URL authorization requires both permissions for NONE auth.
    ignore_exists(lam.add_permission,FunctionName=NAME,StatementId='PublicURL',Action='lambda:InvokeFunctionUrl',
        Principal='*',FunctionUrlAuthType='NONE')
    ignore_exists(lam.add_permission,FunctionName=NAME,StatementId='PublicURLInvoke',Action='lambda:InvokeFunction',
        Principal='*',InvokedViaFunctionUrl=True)
    try:
        url=lam.get_function_url_config(FunctionName=NAME)['FunctionUrl']
    except lam.exceptions.ResourceNotFoundException:
        url=lam.create_function_url_config(FunctionName=NAME,AuthType='NONE')['FunctionUrl']
    s3.put_bucket_notification_configuration(Bucket=bucket,NotificationConfiguration={'LambdaFunctionConfigurations':[
        {'Id':'AutomaticIngest','LambdaFunctionArn':function_arn,'Events':['s3:ObjectCreated:*'],
         'Filter':{'Key':{'FilterRules':[{'Name':'prefix','Value':'incoming/'},{'Name':'suffix','Value':'/recording.zip'}]}}}]})
    publish_samples(s3, bucket)
    result={'bucket':bucket,'region':REGION,'api_url':url}
    (ROOT/'briskapi/archive.json').write_text(json.dumps(result,indent=2)+'\n')
    return result

if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bucket',required=True,help='Use a new dedicated globally unique bucket name')
    args=p.parse_args()
    print(json.dumps(deploy(args.bucket),indent=2))
