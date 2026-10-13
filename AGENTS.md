# Contributor automation guidance

Use Python in `.venv`; install `requirements-dev.txt` for local archive tests.
Do not commit downloaded vendor assets, market recordings, AWS credentials or
upload tickets, identity secrets or broker BRiSK session cookies. The committed
`briskapi/archive.json` contains only bucket, region and API URL. The archive is private:
each uploader reaches only its own uploads; only `samples/` is public. Uploads are on by
default: the CLI asks once on a terminal (Enter accepts) and saves the answer; the Python
API and runs without a terminal upload by default after a once-per-process notice. Every
clean session uploads, broker market recordings and timing reports included, with explicit
opt-outs (`--no-upload`, `--no-upload-market-data`, `BRISK_UPLOAD=0`, `brisk consent --revoke`).
Never archive cookies, tokens or connection diagnostics. New uploads must be decoded;
the existing legacy wire capture is labeled sample_data and is not an ingest format.

Run focused archive and API coverage (minimum 85%), the fixture reference test,
Node tests and Rust tests/fmt/Clippy. CI supplies downloaded demo fixtures. Keep README.md (usage only),
its Japanese translation README.ja.md, ARCHITECTURE.md (internals), PRIVACY.md and
schema documentation synchronized; changes to collected data bump `POLICY_VERSION`.

`infra/deploy.py` mutates AWS resources and is reserved for explicit deployment
work. `cloud_smoke.py` uploads self-authored synthetic data under throwaway identities to
the configured archive, then deletes it. Unit tests must never upload data or mutate cloud
resources.
