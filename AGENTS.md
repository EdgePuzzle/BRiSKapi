# Contributor automation guidance

Use Python in `.venv`; install `requirements-dev.txt` for local archive tests.
Do not commit downloaded vendor assets, market recordings, AWS credentials or
upload tickets, identity secrets or broker BRiSK session cookies. The committed
`briskapi/archive.json` contains only bucket, region, API URL and query URL. The archive is
private: each uploader reaches only its own uploads; only `samples/` is public. The cloud API
(`query_engine.py`, `query_service.py`) runs only in the cloud, like `archive_service.py`: it
is not in the wheel, so Historical's OHLCV bars exist only there (`infra/deploy.py` packages it):
Historical never builds bars from local files. Its record schemas and projections are
`briskapi/records.py`, shared with `briskapi.Live`, which streams the same records from a local
feed's gateway and builds live bars from the stream by the cloud's rules: keep the two equal
(`tests/test_gateway.py` checks), and keep the gateway on 127.0.0.1 behind its owner-only key file. Uploads
are on by default: the CLI asks once on a terminal (Enter accepts) and saves the answer; the Python
API and runs without a terminal upload by default after a once-per-process notice. Every
clean session uploads, broker market recordings and timing reports included, with explicit
opt-outs (`--no-upload`, `--no-upload-market-data`, `BRISK_UPLOAD=0`, `brisk consent --revoke`).
Never archive cookies, tokens or connection diagnostics. New uploads must be decoded;
the existing legacy wire capture is labeled sample_data and is not an ingest format.

Run focused archive, query and API coverage (minimum 85%, `--cov=archive_service
--cov=query_engine --cov=query_service --cov=briskapi`), the fixture reference test,
Node tests and Rust tests/fmt/Clippy. CI supplies downloaded demo fixtures. Keep README.md (usage only),
its Japanese translation README.ja.md, ARCHITECTURE.md (internals), docs/cloud-api.md,
PRIVACY.md, infra/README.md, docs/data-format.md (a test checks it names every API column) and
schema documentation synchronized; changes to collected
data bump `POLICY_VERSION`, and PRIVACY.md also covers how OHLCV bars are built and who
receives them.

`infra/deploy.py` mutates AWS resources and is reserved for explicit deployment
work. `cloud_smoke.py` uploads self-authored synthetic data under throwaway identities to
the configured archive, then deletes it, and calls the cloud API's metadata endpoints. Unit
tests must never upload data or mutate cloud resources.
