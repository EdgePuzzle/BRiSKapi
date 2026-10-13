# Tokyo private archive

The deployed service uses a private S3 bucket, a Python 3.12 ARM64 Lambda with a function
URL and S3 event trigger, and a DynamoDB on-demand quota table. All data, compute and quota
resources are in `ap-northeast-1`; IAM is global. No GitHub secrets are required for
ordinary recording or uploading. Each uploader reaches only its own uploads, by its identity
(see the private archive section of [ARCHITECTURE.md](../ARCHITECTURE.md)).

With operator AWS credentials, provision a **new dedicated** installation:

```sh
.venv/bin/python infra/deploy.py --bucket YOUR_UNIQUE_TOKYO_BUCKET
```

This updates `briskapi/archive.json`. Deployment is repeatable and updates Lambda code.
Resource names are `brisk-recorder-archive` (the project's former name; renaming them
would create a second deployment). Use a separate AWS account or change
`NAME` for a second installation. The deployer needs S3, Lambda, IAM role/policy,
CloudWatch Logs, DynamoDB and STS access. It does not modify default AWS regions.

The execution role can read and write only staging, archive, timing and `reference/`
objects, list the bucket (so that a missing key answers 404, not 403) and object versions only
under `archive/` and `timing/`, delete current
objects in staging, archive and timing, and delete object versions only in staging (so an
owner's delete leaves noncurrent versions it cannot purge, and it can delete nothing under
`reference/`). It also updates the quota table and writes its own logs. The function URL accepts requests
without AWS credentials; each names its owner with an identity secret, and the function
scopes it to that owner's prefixes. S3 POST policies grant only the ticket's particular key
and exact size, and downloads are 15-minute presigned URLs for the owner's own objects.
The bucket blocks public ACLs; its policy allows anonymous reads and listings of `samples/`
only and refuses non-TLS requests. ACLs are disabled, encryption and versioning enabled.
Logs expire after 14 days.

Ingest accepts canonical demo/synthetic recordings that match a reference replay
and decoded broker recordings (SBI, e-shiten) with structural/continuity checks,
and timing reports from either. It
stores the verified Parquet tables, each as its own object (see the integrity and storage
sections of [ARCHITECTURE.md](../ARCHITECTURE.md)). The bucket's prefixes:

| Prefix | Holds |
| --- | --- |
| `incoming/TICKET/` | Staging: a ticket's manifest, owner, status and the uploaded `recording.zip`, the only zip |
| `archive/OWNER/YYYYMMDD/FROM-TO-SHA256/` | A recording's tables, `files.json` (where each is) and, written last, `manifest.json` |
| `reference/SOURCE/TIMELINE/` | A demo or synthetic replay's market tables, kept once, and `complete.json` |
| `timing/OWNER/YYYYMMDD/` | Timing reports |
| `samples/`, `deploy/` | The legacy sample download; the dependency layer |

A demo or synthetic replay's market tables are the same for every recording of it, so the
first verified replay becomes the archive's copy in `reference/`, and each recording of
that replay (the first too) stores only its timing table. Later uploads of the replay send
just that table; until a copy exists the service answers HTTP 409 and the client sends the
whole recording. Nothing is configured for this, and an owner's delete leaves the copy
alone. The synthetic probe of `cloud_smoke.py` leaves its own copy under
`reference/synthetic_test/` the same way. The deployment package holds
`archive_service.py`, `briskapi/schema.py`, `briskapi/_parquet.py` and
`briskapi/references/*.json`, with an empty package init so the client API stays
out of it. pyarrow and numpy come from a layer the deployer builds from their pinned
manylinux arm64 wheels (`LAYER_REQUIREMENTS`; pip downloads them from PyPI) and
publishes as a layer version through the bucket's `deploy/` prefix, once per distinct layer.
The function has 3,008 MB, the most this account's Lambda allows: verifying a complete demo replay holds its decoded
columns in memory (about 2.2 GB at peak) and takes under a minute, a timing-only upload
included, since the service verifies the assembled recording in full either way. Each validated staging upload, and any extra upload made
with an already-used ticket, is deleted on arrival. The deployer creates a random
`QUOTA_SALT` once and keeps it across redeploys, so per-IP quota entries are keyed
hashes that cannot be reversed to addresses.

The deployer reserves concurrency at four when the regional account quota allows
it. The current small Tokyo account quota prevents reserving that capacity; AWS's
account concurrency limit and application quotas still apply. This does not alter
other functions' concurrency allocations. Upload quotas are intentionally modest
and defined in `archive_service.py`; adjust them when usage warrants it.

S3 can take a few minutes to apply a changed notification filter (0.9.0 changed it
from `events.jsonl.gz` to `recording.zip`). An upload in that window is not ingested;
invoke the function with that object's S3 event (key and version) to process it, or
let staging expire it after two days.

Events are version bound, storage uses conditional immutable writes and the
manifest is a commit marker (a replay's `reference/` copy has `complete.json`).
Infrastructure/storage failures retry via Lambda;
malformed data produces a `rejected` status. The CLI prints a bearer-ticket
status URL and reports a pending upload if its wait expires. Operators can
inspect `/aws/lambda/brisk-recorder-archive` for infrastructure failures. Rejected
recordings are never stored.

Costs accrue to the operator: storage, data transfer, Lambda requests/compute and
DynamoDB requests. Staging expires; stored data is kept until its owner deletes it, and a
deleted upload for 31 days after that (below). The
daily upload budget controls ticket issuance, not lifetime archive size or read traffic.

An owner's delete (`brisk delete`) removes the current objects, and the versioned bucket keeps
the earlier versions as noncurrent ones. `brisk list --deleted` shows what can still be
restored and `brisk restore` brings it back, for 30 days (`RESTORE_DAYS` in
`archive_service.py`). The lifecycle rules `deploy.py` sets, `ExpireDeleted-archive` and
`ExpireDeleted-timing`, remove noncurrent versions under `archive/` and `timing/` 31
days after the delete (`DELETED_DAYS`) and expired delete markers, so a deleted upload is gone a day after its
restore window closes; S3 applies lifecycle rules asynchronously, so removal can trail that by
a day. Staging has its own rule (`ExpireStaging`: two days). Deployment replaces the bucket's
whole lifecycle configuration, so a rule added by hand does not survive it, and the new rules
also reach noncurrent versions that exist when they are first deployed: those older than 31
days go at the next lifecycle run.

Making the bucket private (policy version 6) left the objects stored while it was public
in place, outside any owner's prefix: no one can reach them through the function, and the
operator removes them on request or with the AWS console. Each deployment copies the legacy
wire-format sample (`brisk-sbi-wire-jsonl-v1`) among them to `samples/DATE/SHA256/`, the
public download (`publish_samples` in `deploy.py`; idempotent, the originals stay).

To stop new uploads, disable/delete the function URL or set reserved
concurrency to zero; owners then can't reach their uploads either. For complete
removal: remove bucket notifications, delete the Lambda/URL, quota table, log group
and execution role/inline policy, then remove all bucket object versions and delete
the bucket. Removing the versioned bucket is an explicit destructive operation;
the deployer does not do it automatically.
