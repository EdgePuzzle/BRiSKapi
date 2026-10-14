# Contributing

Software contributions: open a pull request with the behavior change, relevant
verification and updated docs. Keep vendor assets, captures, account/session
material, build products and cloud credentials out of Git. Do not commit AWS
upload tickets; they are short-lived bearer credentials.

Recording uploads go to each user's private archive automatically, by default (the CLI
asks once on a terminal; `brisk consent` changes it); see `PRIVACY.md`. Tests run offline
(`tests/conftest.py` refuses non-local requests), so a test that records or connects
patches the upload calls or passes `upload=False`. Any change to what is collected must
update that policy and bump `POLICY_VERSION` in `briskapi/cli.py`, so saved choices lapse
and users are asked again. The service checks the exact tables and their canonical
encoding, the schema, integrity, continuity and the reference replay, then stores the
verified tables under the uploader's owner ID (a demo replay's market tables are kept once,
so such an upload sends only its timing table). Do not include account,
authentication or order information. The accepted decoded schema is documented in
`briskapi/schema.py` and its tables in `briskapi/_parquet.py`; it contains
market/auction data and local timing only.

Data quality metadata includes source, JST trading date and source-time range,
security coverage, batches, quote count and expanded (canonical JSON) size. The
SHA-256 is that of the deterministic zip of the four tables as stored.
Distinct receipt clocks remain distinct recordings; identical payloads
deduplicate by hash. `briskapi/references/historical_mock.json` pins the demo replay; it
changes only with a re-audited asset pin (`tools/brisk_mock/build_reference.py`).

## Development

Work on free-threaded Python 3.14 (3.14t), on which a whole-market live feed keeps up, as CI
does. A python.org build of it on macOS loads no CA certificates until its
"Install Certificates" step has run; briskapi's own requests use the system's then (`briskapi/_https.py`).

```sh
uv venv --seed --python 3.14t .venv
.venv/bin/python -m pip install -r requirements-dev.txt -e .
.venv/bin/python -m pytest tests \
  --cov=archive_service --cov=query_engine --cov=query_service --cov=briskapi --cov-fail-under=85
.venv/bin/python tools/brisk_mock/download_mock.py --cache /tmp/brisk-mock-cache
BRISK_MOCK_CACHE=/tmp/brisk-mock-cache .venv/bin/python -m pytest tools/brisk_mock/test_reference.py
BRISK_MOCK_CACHE=/tmp/brisk-mock-cache node --test tools/brisk_mock/*.test.cjs
BRISK_MOCK_CACHE=/tmp/brisk-mock-cache cargo test --locked \
  --manifest-path rust/brisk_quote_ingest/Cargo.toml
cargo fmt --manifest-path rust/brisk_quote_ingest/Cargo.toml --check
cargo clippy --locked --manifest-path rust/brisk_quote_ingest/Cargo.toml --all-targets -- -D warnings
```

Ordinary tests are local and never touch the cloud. `BRISK_MOCK_CACHE` opts into
fixture replay. CI runs all of the above. Aim for at least 85% coverage of new
behavior.

Keep `README.md` and `README.ja.md` in step; internals belong in `ARCHITECTURE.md`.
Everything the installed package needs at runtime lives in `briskapi/` (package
data is listed in `pyproject.toml`); CI installs the wheel outside the repository
and runs it. The cloud API's engine and service (`query_engine.py`, `query_service.py`)
run only in the cloud, like `archive_service.py`, and are not in the wheel; its client is
`briskapi/_historical.py`, and `tests/test_query.py` covers all three offline against the
archive tests' in-memory S3 and DynamoDB. Their record schemas are `briskapi/records.py`, which
ships in the wheel because `briskapi.Live` projects the same records from a local feed;
`tests/test_gateway.py` runs a session through a feed's gateway and checks every live schema
against the cloud API's records for it. `PRIVACY.md` describes each owner's OHLCV bars, built
from its own broker recordings, so a change to what they hold or who receives them (`build_bars`,
`covered` in `query_engine.py`) updates it like any change to what is collected. The BRiSK site client's tests (`tests/test_brisk.py`, `tools/brisk_mock/brisk.test.cjs`) use
pybrisk's SBI sample payloads and a fake server. Unless a user
opts out, broker sessions (SBI, e-shiten) upload a timing report
(`briskapi/timing.py`, validated by `validate_timing` in `briskapi/schema.py`) and, after each
day's clean end, their market recording; `--no-upload-market-data` keeps the recording local and
`--no-upload` both. Live market content has no reference replay; it passes structural
and continuity checks, while its accuracy remains uploader-declared.
Never archive cookies, tokens or connection diagnostics. New uploads must be
decoded; the existing legacy wire capture is labeled `sample_data` and is not an
ingest format.

## Documentation site

`.github/workflows/pages.yml` publishes https://honvl.github.io/BRiSKapi/ from `main`: the README's
sections as guides, the other documents as pages (their `.ja.md` translations as the Japanese site's)
and an API reference generated from `docs/site/reference.py`, in Japanese from `docs/site/reference.ja.json`
(the build refuses a method or parameter it doesn't translate). Its look is `docs/site/extra.css`, with
`docs/site/overrides` for the fonts and the version in the header. Each reference page shows its example's
real output, kept in `docs/site/responses.json` (the home page shows the demo's one-minute bars from
there too); after changing an example, run it again:

```sh
python3 -m venv .venv-docs && .venv-docs/bin/python -m pip install -r docs/site/requirements.txt
.venv-docs/bin/python docs/site/build.py --serve      # http://127.0.0.1:8000; --strict, as in CI
.venv/bin/brisk record --speed 0 --no-upload --output /tmp/brisk-demo
.venv/bin/python docs/site/capture.py --demo /tmp/brisk-demo --only python/ticker_quote
```

`capture.py` queries the public demo dataset with a throwaway identity it never saves and uploads
nothing. Examples that need a broker login or your own uploads carry written, illustrative responses.

## Releases

1. Once, on pypi.org: add a trusted publisher for project `briskapi`, repository
   `honvl/BRiSKapi`, workflow `release.yml`, environment `pypi`. In the GitHub
   repository settings, create the `pypi` environment.
2. Set `version` in `pyproject.toml`, commit, then push a matching tag
   (`git tag v0.2.0 && git push origin v0.2.0`).

`.github/workflows/release.yml` tests and builds the wheel and sdist, builds the
Rust tools for Linux (x86-64, ARM64), macOS (Apple silicon) and Windows with
their decoder host, creates the GitHub Release with checksums and publishes to
PyPI. Running the workflow manually is a dry run that publishes nothing.

`cloud_smoke.py` is an opt-in real cloud test against the configured archive. It uploads
only the fixed synthetic fixture under throwaway identities, checks that another identity
and anonymous requests reach none of it (anonymous requests reach only `samples/`) and that
tampered content is rejected, then deletes it from the owner's archive. With the cloud API
deployed it also calls two metadata endpoints as a throwaway identity and checks that a
request without a key is refused. Archive and cloud API changes reach clients only after `infra/deploy.py` runs (see `infra/README.md`).
