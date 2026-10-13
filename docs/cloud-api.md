# Cloud API

A Databento-style historical API over each uploader's private archive: query your own
recordings by dataset, symbols, schema and time range, across days, without downloading whole
recordings. Status: phase 1 is built for 1.0 (see [Phases](#phases)); phases 2 and 3 are
plans. It answers once `infra/deploy.py` has deployed the `brisk-recorder-query` function.
Facts about Databento below were checked against its documentation and the `databento-python`
source on 2026-10-13. The code is `query_engine.py`, `query_service.py` and
`briskapi/_historical.py`; [ARCHITECTURE.md](../ARCHITECTURE.md) describes how it works inside
and the [README](../README.md) how to use it.

Decided (2026-10-13):
- our own formats (Parquet, CSV, JSON), not Databento's binary DBN;
- free, with quotas;
- your archive identity is your API key;
- the datasets are `XTKS.BRISK` and `XTKS.BRISKDEMO`;
- OHLCV bars are built once per trading day from every upload (the shared bars), and an
  identity receives only the bars its own uploads cover (moved into phase 1);
- requests filter and sort on `ts_event`, and times without a zone are JST.

## Goals

- **Your data, queryable.** One request returns, for example, Toyota's trades or ten-level book
  for a week of your BRiSK sessions, merged and time-ordered, as a DataFrame-ready table.
- **Databento's shape.** The same endpoints, parameters, dataset naming and record schemas, so
  that code and habits carry over.
- **Private by construction.** A request reaches only its owner's recordings, plus the public
  demo for any identity. The one thing built from everyone's uploads is the OHLCV bars, which
  hold only prices, volumes and times, and an identity receives a bar only where its own
  uploads cover it.
- **Cheap to run.** Serverless, reading only the Parquet row groups a request needs; no database.

Non-goals: live streaming (the feed is `briskapi.connect`), other owners' records, SQL, DBN for
now.

## What Databento's historical API is, and what is built

Base URL `https://hist.databento.com/v0/`; the API key is the HTTP basic-auth username with an
empty password. Ours is a function URL with the same `/v0/` paths and key convention (a Bearer
token works too).

| Endpoint | Purpose | Here |
| --- | --- | --- |
| `POST timeseries.get_range` (form body) | The data: `dataset`, `symbols` (comma-joined, up to 2,000, or `ALL_SYMBOLS`), `schema`, `start` (inclusive), `end` (exclusive), `stype_in`, `stype_out`, `limit`, `encoding` (`dbn`, `csv`, `json`), `compression`, and for CSV/JSON `pretty_px`, `pretty_ts`, `map_symbols`. Filters on `ts_recv` where the schema has it, else `ts_event`. | Built, as a POST form or a GET. No `stype_out` (`stype_in` is `raw_symbol` only) and no `dbn`: `encoding` is `parquet` (the default), `csv` or `json`. Filters on `ts_event`. |
| `GET metadata.list_datasets`, `list_schemas`, `list_fields`, `list_publishers` | Catalogue. Publishers pair a dataset with a venue: `{publisher_id, dataset, venue, description}`. | Built. `list_fields` gives each column's name, type and meaning ([data-format.md](data-format.md)). |
| `GET metadata.get_dataset_range`, `get_dataset_condition` | Available dates; per-day quality. | Built. The range is the first and last date with data; the condition is `available` or `missing` per date, with the clock windows (JST) your recordings cover. |
| `GET metadata.get_record_count`, `get_billable_size`, `get_cost` | Size and price of a request before running it. | `get_record_count` runs the query and counts its rows; `get_cost` is 0. `get_billable_size` is phase 3. |
| `GET symbology.resolve` | Map symbols between types (`raw_symbol`, `instrument_id`, ...) over a date range. | Phase 2. |
| `POST batch.submit_job`, `GET batch.list_jobs`, `batch.list_files` (+ download) | Large requests as asynchronous jobs whose files are downloaded later. | Phase 3. |

Datasets are named `VENUE.FEED`: the venue's ISO 10383 market identifier code (MIC), then the
feed. Examples: `XNAS.ITCH` (Nasdaq TotalView-ITCH), `GLBX.MDP3` (CME Globex MDP 3.0),
`XNYS.PILLAR`, `XCIS.TRADESBBO`. Records carry `ts_event` and, where captured, `ts_recv`
(nanoseconds, UTC), `instrument_id` (uint32) and `publisher_id`.

## Datasets, symbols and instruments

Following that convention, the venue is the Tokyo Stock Exchange (MIC `XTKS`, as in our
NautilusTrader instrument IDs such as `7203.XTKS`) and the feed is BRiSK:

| Dataset | Data | Who can query it |
| --- | --- | --- |
| `XTKS.BRISK` | Every live recording you uploaded, whichever broker's BRiSK it came through; its OHLCV bars are the shared bars | You |
| `XTKS.BRISKDEMO` | The public BRiSK Next demo (TSE, 2021-09-27), from the archive's one verified copy | Every identity |

- **One live dataset.** The broker site a recording came through doesn't matter: every site runs
  the same BRiSK app, which serves the same boot, master, snapshot and decoder (checked on
  2026-10-08). Overlapping captures of a day, from any site, are merged and deduplicated (see
  below).
- **The public demo is open to every identity.** `XTKS.BRISKDEMO` reads
  `reference/historical_mock/TIMELINE/`, the market tables kept once for everyone, so any
  identity can query it, whether or not it uploaded a replay. A replay has no meaningful local
  receipt time, so its records carry `ts_recv = ts_event`. The copy appears with the first
  verified whole-recording demo upload from any identity; `deploy.py --seed-demo PACKAGE`
  makes one at deployment. Until then `XTKS.BRISKDEMO` answers HTTP 404. Its bars are built
  from the demo's trades on the fly and are not gated.
- `list_datasets` returns both datasets, always. `list_publishers`, in Databento's form:

  | `publisher_id` | `dataset` | `venue` | `description` |
  | --- | --- | --- | --- |
  | 1 | `XTKS.BRISK` | `XTKS` | Tokyo Stock Exchange via BRiSK |
  | 2 | `XTKS.BRISKDEMO` | `XTKS` | Tokyo Stock Exchange, BRiSK Next demo replay |

- `raw_symbol` is the JPX code (`7203`, `167A`): four or five characters, 0-9 and A-Z.
  `instrument_id` has to stay the same across days, but a recording's `issue_id` is only an
  index into that day's master. So the ID is derived from the code:
  - digits-only codes are their number (Toyota stays 7203);
  - alphanumeric ones (JPX has issued them since 2024) are 100,000 plus their base-36 value
    (`167A` → 100,000 + 54,694 = 154,694).

  Codes have at most five characters, so this is unique and fits in uint32.
  `symbology.resolve` (phase 2) will report both, per date.

## Schemas from BRiSK recordings

Recordings already hold everything needed; each schema is a projection or a small derivation.
Field names follow Databento's record definitions, so a DataFrame of `trades` or `mbp-10` has
the columns a Databento user expects. Every schema but the bars starts with `ts_recv`,
`ts_event`, `publisher_id` and `instrument_id` and ends with `symbol`.

| Schema | From | Mapping |
| --- | --- | --- |
| `trades` | `data/trades.parquet` | `action` T; price, size; `side` from the aggressor (`B` buyer-initiated, `A` seller-initiated, `N` otherwise, as for most opening prints); `sequence` = frame; `trade_id` |
| `mbp-10` | `data/order_book_depths.parquet` | Each quote update is the ten best levels per side after it, flattened to `bid_px_00`, `bid_sz_00`, `bid_ct_00` … `ask_ct_09` (null past a side's last level). BRiSK gives the book, not the order that changed it, so `action` and `side` are `N`, `depth` is 0, `price` null and `size` 0. The bootstrap batch's rows carry the snapshot flag (`flags` 32). |
| `imbalance` | the auction fields | BRiSK's strength: per update an opening (`O`) row where the opening indicative price shows and a closing (`C`) row where the closing one does, in `auction_type`. `ind_match_price` = indicative price, `paired_qty` = indicative volume, `market_imbalance_qty`/`_side` = market buy minus sell, `bid_total_qty`/`ask_total_qty` = the board totals (the closing ones for `C`), collars = the day's limit prices, `auction_time` = special-quote timer. |
| `definition` | `data/equity.parquet` | One row per security per recording, stamped JST midnight of its trading day: code, name, lot size, tick increment, base price, limits, issue and tick types |
| `brisk-quotes` (ours) | depths, all columns | Everything BRiSK records for each update: closing-condition books, board totals, special-quote time (decimals as float64) |
| `ohlcv-1s/1m/1h/1d` | trades | Aggregated from trades, as Databento does; see [below](#ohlcv-bars-and-the-shared-bars) |

Planned for phase 2:

| Schema | From | Mapping |
| --- | --- | --- |
| `mbp-1`, `bbo-1s/1m`, `tbbo` | the same depths | Top level; sampled; or each trade with the book before it |
| `status` | `issue_status`, special quotes | Trading status changes (special quote on/off, halts as BRiSK reports them) |
| `statistics` | open, last, volume, turnover | Databento `stat_type`s where one fits; turnover as a BRiSK extension |

**Encodings.**
- `parquet` (the default for our client): timestamps as UTC nanoseconds
  (`timestamp[ns, UTC]`), prices as float64 yen. Always zstd; `compression` does not apply.
- `csv` and `json` (one record per line), with Databento's `pretty_px` (prices as numbers in
  yen; off, integers in units of 1e-9 yen, Databento's fixed point), `pretty_ts` (ISO 8601 UTC
  timestamps with nanoseconds; off, integer nanoseconds) and `map_symbols` (keep the code)
  switches. Nested columns of `brisk-quotes` are JSON text in CSV.
- `compression=zstd` optional for CSV and JSON.

**Timestamps and times.**
- `ts_event` is BRiSK's server stamp for the frame (a trade's own time for trades). Whether that
  is the exchange's event time is not established (see ARCHITECTURE.md), and the docs must say
  so.
- `ts_recv` is the uploader's local receipt time (`received_unix_ms` of the frame's batch;
  millisecond precision), or `ts_event` where there is none.
- Requests filter and sort on `ts_event`, not on `ts_recv` as Databento does: overlapping
  captures of one day agree on BRiSK's stamp and differ in their receipt times, so the same
  range returns the same rows whichever capture holds them. Ties sort by `instrument_id`, then
  `sequence`.
- `start` and `end` are ISO 8601, a date or an integer. A date is that JST trading day's
  midnight (so an `end` date excludes the day), a time without a zone is JST, `Z` or an offset
  is honoured, and an integer of ten or more digits is UTC nanoseconds. `end` is exclusive and
  defaults to the end of the start's trading day. A request touches at most 31 JST trading
  dates.

## OHLCV bars and the shared bars

Bars come only from the cloud: the engine (`query_engine.py`) is server side and not in the
`briskapi` wheel.

- **Building.** From trades, per security and interval. Intervals align to JST (a day bar runs
  JST midnight to midnight); a bar's `ts_event` is its start; an interval without a trade has
  no bar. Open and close are the first and last trade by `ts_event`, then frame, then place in
  the frame; `volume` is the sum of sizes (uint64). Auction prints are trades like any other.
  Columns: `ts_event`, `publisher_id`, `instrument_id`, `open`, `high`, `low`, `close`,
  `volume`, `symbol`.
- **Central store.** For `XTKS.BRISK` one set of bars per trading day is built from every
  current live recording of that day, whoever uploaded it, with each trade counted once
  (deduplicated by security and trade ID). It lives in
  `central/XTKS.BRISK/YYYYMMDD/ohlcv-1s|1m|1h|1d.parquet`, sorted by security so that a query
  reads only its securities' row groups. A marker per backing recording sits under
  `sources/`. The archive service rebuilds the day when a live recording is stored, deleted
  or restored; with no marker left the files are removed, and a rebuild that fails removes
  them too (queries then build the owner's bars from its own recordings, as they do for a day
  with no central files yet).
- **Entitlement.** The bars hold no identity or local timing, but they hold everyone's trades,
  so a request receives a bar only if its span, clipped to the Tokyo Stock Exchange's trading
  sessions (09:00 to 11:30:01 and 12:30 to 15:30:01 JST, half open, the extra second holding
  the closing auction's prints), lies within the merged clock ranges (first to last source
  clock) of your own recordings that hold that security. A bar outside every session is judged
  on its whole span. A recording from 08:55 to 11:31, for example, gets the morning's
  one-minute and one-hour bars but not the day bar. So uploading is how you get bars.
  [PRIVACY.md](../PRIVACY.md) (policy version 7) describes the shared bars to uploaders.

## Request flow

```
client ──HTTPS──▶ Lambda function URL "brisk-recorder-query" (Python 3.12 arm64, 3,008 MB, pyarrow layer)
                    │ 1 authenticate: the identity (Basic username or Bearer) → owner ID; count the request
                    │ 2 plan: for each JST trading date in [start, end), list archive/OWNER/DATE/ and read
                    │   each live recording's manifest and files.json
                    │   (XTKS.BRISKDEMO: the reference copy instead)
                    │ 3 resolve symbols per recording from its equity table (code → issue_id)
                    │ 4 read: pyarrow over S3 ranged GETs (4 MiB blocks), only row groups whose issue_id and
                    │   ts_event statistics can match (tables are sorted by issue, then batch);
                    │   bars: the day's central file, row groups pruned by symbol
                    │ 5 merge recordings, deduplicate, sort by ts_event, project to the schema, encode;
                    │   bars: keep what the owner's coverage allows
                    │ 6 count the result's bytes against the daily quota
                    ▼
              ≤ 4 MiB: inline response (Parquet as base64)
              larger: results/OWNER/<uuid>.<ext> in S3, then 303 to a 15-minute presigned URL
```

- **Several recordings of one day** (a restart, two computers, two sites) can overlap. Rows are
  merged and deduplicated, keeping the first, by (code, trade ID) for trades, (code, time) for
  definitions, (code, frame, auction type) for imbalance and (code, frame) otherwise. BRiSK's
  frame numbers are the server's, so overlapping captures of the stream should agree; to be
  confirmed on live data, across sites included.
- **Row-group pruning.** Writing a Parquet page index (`write_page_index`) would make
  single-symbol reads finer still. It changes the canonical encoding, so it needs a schema version
  bump on client and service together.
- **Why 303 and not streaming.** Function URLs return at most 6 MB buffered, and base64 adds a
  third to Parquet, hence 4 MiB inline. Response streaming needs the Node.js runtime or a custom
  one (to re-check), while this code is Python and pyarrow. A 303 to S3 serves any size, and
  HTTP clients follow it. `requests` drops the Authorization header on a cross-host redirect,
  which S3 presigned URLs require, and `briskapi.Historical` follows it by hand without the key.
  Results expire after a day (lifecycle); each large result is its own object.
- **Lambda limits.** 300 seconds and 3,008 MB (the account's maximum) per request; anything
  bigger is a batch job (phase 3). With no recording in range, a result holds only the common
  columns and `symbol`.

## Authentication and isolation

- **The identity is the API key**, sent as the HTTP basic-auth username with an empty password
  (Databento's convention, so `curl -u $KEY:` works) or as `Authorization: Bearer` (as the
  archive API already does). There are no separate keys to create: `brisk identity --export`
  prints it, and `briskapi.Historical()` finds it as the archive does (`BRISK_IDENTITY`, the
  credential store or the file). The same key can also upload, delete and restore, so it should be
  kept like a password. There is no key registry: any well-formed identity authenticates, and one
  with no recordings sees only the demo. A missing or malformed key is HTTP 400.
- **Isolation.** The query function's role may read `archive/*`, `reference/*`, the bar files
  under `central/` and `results/*`, and write `results/*`, nothing else. Every path into your recordings is built from
  the authenticated owner ID, never from request input. The other prefixes a request reads are
  the demo's reference copy, for `XTKS.BRISKDEMO`, and the day's central bars, for `XTKS.BRISK`
  bars, which are cut to the owner's coverage as above.

## Limits and cost

- **Free, with quotas:** 60 requests a minute and 2 GB returned a UTC day per identity; since
  identities cost nothing to make, also 120 requests a minute per IP address and 20 GB returned a
  day for everyone. They use the same DynamoDB counters as uploads (a request over a quota gets
  HTTP 429). Up to 2,000 symbols and 31 days per request, and `limit`. Results up to 4 MiB are returned inline.
- `get_cost` returns 0. `get_record_count` returns the real number by running the query, so it
  costs the operator a query but no returned bytes. `get_billable_size` (phase 3) will come from
  Parquet metadata.
- **Operator cost per request** (Tokyo list prices):
  - Lambda arm64 is about $0.0000133 per GB-second, so 3 GB for 1 s is about $0.00004.
  - S3 GETs cost $0.00037 per thousand.
  - Data out costs about $0.114 per GB beyond AWS's free 100 GB a month.
  - Egress dominates: a user at the daily cap every day is about $7 a month.

## Python client

```python
import briskapi

client = briskapi.Historical()                 # your identity is the key
data = client.timeseries.get_range(dataset="XTKS.BRISK", symbols=["7203"], schema="trades",
                                   start="2026-10-13", end="2026-10-17")
data.to_df()                                   # indexed by ts_recv (ts_event for bars); Databento's other conversions too
client.timeseries.get_range(dataset="XTKS.BRISK", symbols=["7203"], schema="ohlcv-1m",
                            start="2026-10-13")                 # bars where your uploads cover them
client.metadata.get_dataset_condition("XTKS.BRISK", start_date="2026-10-01")   # recorded windows per day
client.timeseries.get_range(dataset="XTKS.BRISKDEMO", symbols=["7203"], schema="imbalance",
                            start="2021-09-27T08:59+09:00", end="2021-09-27T09:03+09:00")   # any identity
```

Same names and parameters as `databento.Historical`, returning a store with `to_df()`,
`to_ndarray()`, `to_csv()`, `to_json()`, `to_parquet()`, `to_file()`, `replay()`, iteration and `len()`, with `to_df()`'s `price_type`, `pretty_ts`, `map_symbols`, `tz` and `count`. It transports Parquet, so no new dependency. The
gateway comes from `query_url` in `archive.json`.

On the command line, `brisk query XTKS.BRISK --schema ohlcv-1m --symbols 7203 --start 2026-10-13`
writes CSV to stdout (`--encoding json|parquet`, `--output FILE`, `--end`, `--limit`). Over
plain HTTP, `-L` follows the 303 for a large result and curl drops the key on the way:

```sh
curl -L -u "$KEY:" -d dataset=XTKS.BRISK -d schema=ohlcv-1m -d symbols=7203 \
     -d start=2026-10-13 -d encoding=csv "$QUERY_URL/v0/timeseries.get_range"
```

Errors are `{"detail": ...}` with HTTP 400 (invalid input, no key), 404 (no data, or no demo
copy yet) or 429 (quota).

## Phases

1. **Core (built for 1.0).**
   - `timeseries.get_range` for `trades`, `mbp-10`, `definition`, `imbalance`, `brisk-quotes`
     and `ohlcv-1s/1m/1h/1d`, in Parquet, CSV or JSON.
   - The shared bars: the central store, kept in step on store, delete and restore, and the
     per-identity coverage rule (moved up from phase 2).
   - The metadata endpoints (`list_*`, `get_dataset_range`, `get_dataset_condition`,
     `get_record_count`, `get_cost`).
   - Identity auth, inline or 303 results, quotas, `briskapi.Historical`, `brisk query` and the
     demo reference seeded at deployment (`deploy.py --seed-demo`).
2. **More schemas.** `mbp-1`, `tbbo`, `bbo-*`, `status` and `statistics`, plus
   `symbology.resolve`.
3. **Large requests.**
   - `batch.submit_job`, `list_jobs` and `list_files`: asynchronous Lambda invocations writing to
     `results/OWNER/JOB/`.
   - `get_billable_size` and the Parquet page index.

Later, if wanted: DBN encoding via `databento-dbn`, which would let the official `databento`
client use the archive directly (`databento.Historical(key, gateway=URL)`). Its client accepts any
non-empty key and any https gateway.

Known gaps in phase 1:
- Rebuilds of one day's bars are not serialized; one that finds the day's sources changed while
  it ran runs again, up to three times.
- That overlapping captures agree, across sites, is unconfirmed on live data.

## Infrastructure

- A second function, `brisk-recorder-query`, with its own role (`brisk-recorder-query`),
  log group and function URL, in the same zip and layer as the archive function: 3,008 MB (the
  account's maximum), a 300 second timeout and no reserved concurrency. Ingest is unchanged
  except that it keeps the central bars in step, so the archive role gains read, write and
  delete of current objects under `central/`.
- Bucket: a `results/` prefix with a one-day lifecycle (`ExpireResults`); `central/` noncurrent
  versions expire after a day (`ExpireReplacedBars`). The query role reads `archive/`,
  `reference/`, `central/` and `results/` and writes `results/`.
- DynamoDB: query quotas in the existing table.
- `deploy.py` deploys both functions and writes `query_url` to `archive.json`; `--seed-demo
  PACKAGE` uploads a complete demo recording as the operator so that `XTKS.BRISKDEMO` has data
  (`brisk record --web --no-upload --output DIR` makes one). `cloud_smoke.py` calls
  `list_datasets` and `list_publishers` as a throwaway identity and checks that a request
  without a key is refused; it does not query data, since its synthetic probe belongs to
  neither dataset. Deployment steps are in [infra/README.md](../infra/README.md).
