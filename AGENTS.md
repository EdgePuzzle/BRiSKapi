# Contributor automation guidance

Use Python in `.venv`; install `requirements-dev.txt` for local archive tests.
Do not commit downloaded vendor assets, market recordings, AWS credentials or
upload tickets, identity secrets or broker BRiSK session cookies. The committed
`briskapi/archive.json` contains only bucket, region, API URL and query URL. The archive is
private: each uploader reaches only its own uploads; only `samples/` is public. The cloud API
(`query_engine.py`, `query_service.py`) runs only in the cloud, like `archive_service.py`: it
is not in the wheel, so OHLCV bars exist only there (`infra/deploy.py` packages it). Uploads
are on by default: the CLI asks once on a terminal (Enter accepts) and saves the answer; the Python
API and runs without a terminal upload by default after a once-per-process notice. Every
clean session uploads, broker market recordings and timing reports included, with explicit
opt-outs (`--no-upload`, `--no-upload-market-data`, `BRISK_UPLOAD=0`, `brisk consent --revoke`).
Never archive cookies, tokens or connection diagnostics. New uploads must be decoded;
the existing legacy wire capture is labeled sample_data and is not an ingest format.

Run focused archive, query and API coverage (minimum 85%, `--cov=archive_service
--cov=query_engine --cov=query_service --cov=briskapi`), the fixture reference test,
Node tests and Rust tests/fmt/Clippy. CI supplies downloaded demo fixtures. Keep README.md (usage only),
ARCHITECTURE.md (internals), docs/cloud-api.md, PRIVACY.md, infra/README.md, docs/data-format.md (a test
checks it, and its translation, name every API column) and schema documentation synchronized, together with
the Japanese translations README.ja.md, PRIVACY.ja.md, docs/cloud-api.ja.md and docs/data-format.ja.md
(the English PRIVACY.md governs); changes to collected
data bump `POLICY_VERSION`, and PRIVACY.md also covers how OHLCV bars are built and who
receives them. The documentation site (`docs/site`, GitHub Pages) is built from these documents; when
an API reference example changes, rerun `docs/site/capture.py` (it queries the demo dataset with a
throwaway identity and uploads nothing).

`infra/deploy.py` mutates AWS resources and is reserved for explicit deployment
work. `cloud_smoke.py` uploads self-authored synthetic data under throwaway identities to
the configured archive, then deletes it, and calls the cloud API's metadata endpoints. Unit
tests must never upload data or mutate cloud resources.
