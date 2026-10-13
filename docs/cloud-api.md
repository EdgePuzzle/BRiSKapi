# Cloud API design (draft)

A Databento-style historical API over each uploader's private archive: query your own
recordings by dataset, symbols, schema and time range, across days, without downloading whole
recordings. Status: design; nothing here is built yet. Facts about Databento below were checked
against its documentation and the `databento-python` source on 2026-10-13.

Decided (2026-10-13):
- our own formats (Parquet, CSV, JSON), not Databento's binary DBN;
- free, with quotas;
- your archive identity is your API key;
- the datasets are `XTKS.BRISK` and `XTKS.BRISKDEMO`.

## Goals

- **Your data, queryable.** One request returns, for example, Toyota's trades or ten-level book
  for a week of your BRiSK sessions, merged and time-ordered, as a DataFrame-ready table.
- **Databento's shape.** The same endpoints, parameters, dataset naming and record schemas, so
  that code and habits carry over.
- **Private by construction.** A request reaches only its owner's recordings, plus the public demo
  for any identity. Nothing crosses owners.
- **Cheap to run.** Serverless, reading only the Parquet row groups a request needs; no database.

Non-goals: live streaming (the feed is `briskapi.connect`), cross-owner data, SQL, DBN for now.

## What Databento's historical API is

Base URL `https://hist.databento.com/v0/`; the API key is the HTTP basic-auth username with an
empty password. Endpoints:

| Endpoint | Purpose |
| --- | --- |
| `POST timeseries.get_range` (form body) | The data: `dataset`, `symbols` (comma-joined, up to 2,000, or `ALL_SYMBOLS`), `schema`, `start` (inclusive), `end` (exclusive), `stype_in`, `stype_out`, `limit`, `encoding` (`dbn`, `csv`, `json`), `compression`, and for CSV/JSON `pretty_px`, `pretty_ts`, `map_symbols`. Filters on `ts_recv` where the schema has it, else `ts_event`. |
| `GET metadata.list_datasets`, `list_schemas`, `list_fields`, `list_publishers` | Catalogue. Publishers pair a dataset with a venue: `{publisher_id, dataset, venue, description}`. |
| `GET metadata.get_dataset_range`, `get_dataset_condition` | Available dates; per-day quality. |
| `GET metadata.get_record_count`, `get_billable_size`, `get_cost` | Size and price of a request before running it. |
| `GET symbology.resolve` | Map symbols between types (`raw_symbol`, `instrument_id`, ...) over a date range. |
| `POST batch.submit_job`, `GET batch.list_jobs`, `batch.list_files` (+ download) | Large requests as asynchronous jobs whose files are downloaded later. |

Datasets are named `VENUE.FEED`: the venue's ISO 10383 market identifier code (MIC), then the
feed. Examples: `XNAS.ITCH` (Nasdaq TotalView-ITCH), `GLBX.MDP3` (CME Globex MDP 3.0),
`XNYS.PILLAR`, `XCIS.TRADESBBO`.

Schemas: `mbo`, `mbp-1`, `mbp-10`, `tbbo`, `trades`, `ohlcv-1s/1m/1h/1d`, `definition`,
`statistics`, `status`, `imbalance` (and `bbo`/`cbbo` variants). Records carry `ts_event` and,
where captured, `ts_recv` (nanoseconds, UTC), `instrument_id` (uint32) and `publisher_id`.

## Datasets, symbols and instruments

Following that convention, the venue is the Tokyo Stock Exchange (MIC `XTKS`, as in our
NautilusTrader instrument IDs such as `7203.XTKS`) and the feed is BRiSK:

| Dataset | Data | Who can query it |
| --- | --- | --- |
| `XTKS.BRISK` | Every live recording you uploaded, whichever broker's BRiSK it came through | You |
| `XTKS.BRISKDEMO` | The public BRiSK Next demo (TSE, 2021-09-27), from the archive's one verified copy | Every identity |

- **One live dataset.** The broker site a recording came through doesn't matter: every site runs
  the same BRiSK app, which serves the same boot, master, snapshot and decoder (checked on
  2026-10-08). Overlapping captures of a day, from any site, are merged and deduplicated by frame
  (see below).
- **The demo is public to identities.** `XTKS.BRISKDEMO` reads `reference/historical_mock/TIMELINE/`,
  the market tables kept once for everyone, so any identity can query it, whether or not it
  uploaded a replay. A replay has no meaningful local receipt time, so its records carry
  `ts_recv = ts_event`. The copy appears with the first verified demo upload; the deployment
  seeds it (the smoke test uploads a whole recording, or `deploy.py` gets a step).
- `list_datasets` returns `XTKS.BRISKDEMO` always and `XTKS.BRISK` once you have a live recording.
- `list_publishers`, in Databento's form:

  | `publisher_id` | `dataset` | `venue` | `description` |
  | --- | --- | --- | --- |
  | 1 | `XTKS.BRISK` | `XTKS` | Tokyo Stock Exchange via BRiSK |
  | 2 | `XTKS.BRISKDEMO` | `XTKS` | Tokyo Stock Exchange, BRiSK Next demo replay |

- `raw_symbol` is the JPX code (`7203`, `167A`). `instrument_id` has to stay the same across days,
  but a recording's `issue_id` is only an index into that day's master. So the ID is derived
  from the code:
  - digits-only codes are their number (Toyota stays 7203);
  - alphanumeric ones (JPX has issued them since 2024) are 100,000 plus their base-36 value
    (`167A` → 100,000 + 54,694 = 154,694).

  Codes have at most five characters, so this is unique and fits in uint32. `symbology.resolve`
  reports both, per date.

## Schemas from BRiSK recordings

Recordings already hold everything needed; each schema is a projection or a small derivation.

| Schema | From | Mapping |
| --- | --- | --- |
| `trades` | `data/trades.parquet` | price, size; `side` from the aggressor (`B` buyer-initiated, `A` seller-initiated, `N` most opening prints); `sequence` = frame |
| `mbp-10` | `data/order_book_depths.parquet` | Each quote update is the ten best levels per side after it (`bid/ask_px`, `_sz`, `_ct`). BRiSK gives the book, not the order that changed it, so `action`/`side`/`depth`/`price`/`size` describe the first level that differs from the previous update. The bootstrap rows carry the snapshot flag. |
| `mbp-1`, `bbo-1s/1m`, `tbbo` | the same | Top level; sampled; or each trade with the book before it |
| `ohlcv-1s/1m/1h/1d` | trades | Aggregated from trades, as Databento does |
| `imbalance` | the auction fields | BRiSK's strength: `ind_match_price` = indicative price, `paired_qty` = indicative volume, `market_imbalance_qty`/`side` = market buy minus sell, `auction_type` opening or closing (the closing-condition fields), collars = limit prices, `auction_time` = special-quote timer. |
| `status` | `issue_status`, special quotes | Trading status changes (special quote on/off, halts as BRiSK reports them) |
| `statistics` | open, last, volume, turnover | Databento `stat_type`s where one fits; turnover as a BRiSK extension |
| `definition` | `data/equity.parquet` | Code, name, lot size, tick table, base price, limits |
| `brisk-quotes` (ours) | depths, all columns | Everything BRiSK records: closing-condition books, board totals, special-quote time |

Field names follow Databento's record definitions, so a DataFrame of `trades` or `mbp-10` has
the columns a Databento user expects.

**Encodings.**
- `parquet` (the default for our client): prices as exact decimals, timestamps as UTC
  nanoseconds.
- `csv` and `json` (one record per line), with Databento's `pretty_px` (prices as decimal
  strings in yen) and `pretty_ts` (ISO 8601 timestamps) switches, and `map_symbols` (add the
  code).
- `compression=zstd` optional.

**Timestamps.**
- `ts_event` is BRiSK's server stamp for the frame. Whether that is the exchange's event time is
  not established (see ARCHITECTURE.md), and the docs must say so.
- `ts_recv` is the uploader's local receipt time (`received_unix_ms` of the frame's batch;
  millisecond precision).
- Filtering uses `ts_recv` where it exists, as Databento does.

## Request flow

```
client ──HTTPS──▶ Lambda function URL "brisk-recorder-query" (Python 3.12 arm64, pyarrow layer)
                    │ 1 authenticate: the identity (Basic username or Bearer) → owner ID
                    │ 2 plan: list archive/OWNER/YYYYMMDD/ for the dates in [start, end); the names'
                    │   FROM-TO clocks pick the recordings that overlap, without reading manifests
                    │   (XTKS.BRISKDEMO: the reference copy instead)
                    │ 3 resolve symbols per recording from its equity table (code → issue_id)
                    │ 4 read: pyarrow over S3 ranged GETs, only row groups whose issue_id and
                    │   ts_event statistics can match (tables are sorted by issue, then batch)
                    │ 5 merge recordings, deduplicate, sort by ts, project to the schema, encode
                    ▼
              ≤ 5 MB: inline response
              larger: results/OWNER/<request hash>.<ext> in S3, then 303 to a 15-minute presigned URL
```

- **Several recordings of one day** (a restart, two computers, two sites) can overlap. Rows are
  deduplicated by (code, frame) for quotes and (code, frame, trade index) for trades.
  BRiSK's frame numbers are the server's, so overlapping captures of the stream agree; to be
  confirmed on live data, across sites included.
- **Row-group pruning.** Writing a Parquet page index (`write_page_index`) would make
  single-symbol reads finer still. It changes the canonical encoding, so it needs a schema version
  bump on client and service together.
- **Why 303 and not streaming.** Function URLs return at most 6 MB buffered. Response streaming
  needs the Node.js runtime or a custom one (to re-check), while this code is Python and pyarrow.
  A 303 to S3 serves any size, and HTTP clients follow it. `requests` drops the Authorization
  header on a cross-host redirect, which S3 presigned URLs require. Results expire after a day
  (lifecycle), and an identical request reuses its object.
- **Lambda limits.** 15 minutes and 10 GB per request; anything bigger is a batch job.

## Authentication and isolation

- **The identity is the API key**, sent as the HTTP basic-auth username with an empty password
  (Databento's convention, so `curl -u $KEY:` works) or as `Authorization: Bearer` (as the
  archive API already does). There are no separate keys to create: `brisk identity --export`
  prints it, and `briskapi.Historical()` finds it as the archive does (`BRISK_IDENTITY`, the
  credential store or the file). The same key can also upload, delete and restore, so it should be
  kept like a password.
- **Isolation.** The query function's role may read `archive/*` and `reference/*` and write
  `results/*`, nothing else. Every path into your data is built from the authenticated owner ID,
  never from request input. The only other prefix a request reads is the demo's reference copy,
  for `XTKS.BRISKDEMO`.

## Limits and cost

- **Free, with quotas per identity:** 60 requests a minute, 2 GB returned a day, up to 2,000
  symbols and 31 days per request, and `limit`. Enforced with the same DynamoDB quota counters as
  uploads; a request over quota gets HTTP 429.
- `get_cost` returns 0. `get_billable_size` and `get_record_count` return real numbers from
  Parquet metadata, so clients can size a request against the quota first.
- **Operator cost per request** (Tokyo list prices):
  - Lambda arm64 is about $0.0000133 per GB-second, so 2 GB for 1 s is about $0.00003.
  - S3 GETs cost $0.00037 per thousand.
  - Data out costs about $0.114 per GB beyond AWS's free 100 GB a month.
  - Egress dominates: a user at the daily cap every day is about $7 a month.

## Python client

```python
import briskapi

client = briskapi.Historical()                 # your identity is the key
data = client.timeseries.get_range(dataset="XTKS.BRISK", symbols=["7203"], schema="trades",
                                   start="2026-10-13", end="2026-10-17")
data.to_df()
client.metadata.get_dataset_condition("XTKS.BRISK", start_date="2026-10-01")   # recorded windows per day
client.timeseries.get_range(dataset="XTKS.BRISKDEMO", symbols=["7203"], schema="imbalance",
                            start="2021-09-27T08:59+09:00", end="2021-09-27T09:03+09:00")   # any identity
```

Same names and parameters as `databento.Historical`, returning a store with `to_df()`,
`to_arrow()` and `to_parquet()`. It transports Parquet, so no new dependency.

## Phases

1. **Core.**
   - `timeseries.get_range` for `trades`, `mbp-10`, `definition`, `imbalance` and `brisk-quotes`,
     in Parquet, CSV or JSON.
   - The metadata endpoints (`list_*`, `get_dataset_range`, `get_dataset_condition` from the
     FROM-TO names, `get_record_count`, `get_cost`).
   - Identity auth, inline or 303 results, quotas, the demo reference seeded at deploy, and
     `briskapi.Historical`.
2. **More schemas.** `mbp-1`, `tbbo`, `bbo-*`, `ohlcv-*`, `status` and `statistics`, plus
   `symbology.resolve`.
3. **Large requests.**
   - `batch.submit_job`, `list_jobs` and `list_files`: asynchronous Lambda invocations writing to
     `results/OWNER/JOB/`.
   - `get_billable_size` and the Parquet page index.

Later, if wanted: DBN encoding via `databento-dbn`, which would let the official `databento`
client use the archive directly (`databento.Historical(key, gateway=URL)`). Its client accepts any
non-empty key and any https gateway.

## Infrastructure changes

- A second function, `brisk-recorder-query`, with its own role, memory (up to the account's 3,008 MB), reserved
  concurrency and function URL. Ingest is unchanged.
- Bucket: a `results/` prefix with a one-day lifecycle; the query role reads `archive/` and
  `reference/` and writes `results/`.
- DynamoDB: query quotas in the existing table.
- `deploy.py` deploys both functions and seeds the demo reference; `cloud_smoke.py` gains a query
  against the synthetic probe.
