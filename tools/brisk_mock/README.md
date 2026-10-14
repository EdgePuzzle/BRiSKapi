# BRiSK Next mock auction collector

Account-free development against the [public Next demo](https://next-demo.brisk.jp/),
with a Rust latest-state collector and the demo's actual WASM book decoder hosted
in Node. No browser or Tachibana account is needed to run it. Assets are downloaded
separately, pinned by SHA-256 and never committed to this repository.

From the repository root, using Python from `.venv`:

```sh
.venv/bin/python tools/brisk_mock/download_mock.py --cache /tmp/brisk-mock-cache
cargo build --release --manifest-path rust/brisk_quote_ingest/Cargo.toml
rust/brisk_quote_ingest/target/release/brisk_quote_ingest \
  --cache /tmp/brisk-mock-cache --latest /tmp/brisk-latest.json
```

Node 22+ and Rust 1.92+ are required. There are no
npm dependencies. Replay runs at its recorded pace by default. Add `--speed 0`
for an unpaced benchmark, `--codes 7203,6758,8306,9984,6920,5659` for a basket,
or `--events /tmp/brisk-events.jsonl` for a lossless decoded batch recording.
Missing codes fail explicitly; a 2021 mock cannot cover later listings such as
alphabetic codes. An omitted `--codes` selects the entire mock master dynamically.

## Connect to the public web demo

Use `--web` instead of `--cache` to fetch the pinned decoder, master, snapshot and
frame assets directly from `https://next-demo.brisk.jp/` on each start. No Python
download step or local asset cache is required:

```sh
cargo build --release --manifest-path rust/brisk_quote_ingest/Cargo.toml
rust/brisk_quote_ingest/target/release/brisk_quote_ingest \
  --web --latest /tmp/brisk-web-latest.json
# Or stream decoded batches directly:
node briskapi/decoder/decoder.cjs --web --codes 7203,8306
```

This uses the actual public demo's transport: HTTPS assets followed by locally
scheduled historical frames. Its browser code calls `wsMock()` rather than opening
a market-data WebSocket. There is no live market-data WebSocket to connect to on
this demo. Output remains `source=historical_mock`, dated 2021-09-27, and explicitly
records `input_transport.kind=https_recorded_assets`, the website origin and local
asset-fetch duration. That duration is a startup download measurement, not feed
or exchange latency. Cache mode records `input_transport.kind=local_cache`.

Every downloaded asset must match the existing SHA-256 pin. Requests have a
30-second timeout, redirects are rejected, and each decoded response is limited
to 32 MiB. HTTP errors, interrupted downloads or changed assets stop startup;
Rust marks the book invalid and never publishes it as running. The complete frame
asset is fetched before replay, matching the site's behavior. Web mode does not
save vendor assets to disk or silently fall back to stale cached data.

### Relation to upstream live streaming

Upstream [pybrisk's live reader](https://github.com/obichan117/pybrisk/blob/43603e443df4a412c1aa87cd1baca0848b8ccf1d/scripts/cdp_live_reader.py)
attaches to an authenticated **SBI BRiSK Neo** browser, lets that browser maintain
the live connection and vendor WASM state, and extracts decoded events through
CDP. Its endpoints are fixed to `sbi.brisk.jp`. This is a useful browser-based
live architecture reference; it is not a verified Tachibana BRiSK Next connector.
The reader also polls every 500 ms and extracts QR events, which does not provide
the complete auction state this collector needs.

briskapi's own broker live feed (`briskapi.connect(site=...)`, `brisk live --site sbi` or
`--site e-shiten`) does not use a browser or CDP. `briskapi/decoder/brisk.cjs` logs in
with your session cookies, downloads the site's decoder, master and snapshot, opens the
WebSocket itself and runs the same Node `Decoder` as this demo host (protocol version
18000 for the brokers' BRiSK: SBI, e-shiten). It is experimental and has not been
validated against a live broker session.

Its 18000 master record layout, the session schedule and the catch-up can be checked
offline, without an account:

```sh
.venv/bin/python tools/brisk_mock/sbi_master_layout.py --cache /tmp/sbi-master-cache
```

This downloads pinned copies of SBI's decoder of March 2026 and, from pybrisk's
capture of sbi.brisk.jp, the master file, the boot's `schedule_info` and the catch-up
request's issue lists with its reply (only these are taken out of the capture, which
also holds their expired session). It then runs the `Decoder` on them under Node's
permission model and prints a report: issues, record size, agreement with the master
file, tick types, a limit change read back, and the catch-up with and without the
schedule (price bars, issues advanced, stock views that read back the decoder's frame
numbers, and whether briskapi's catch-up request is the vendor client's), and the
archive's copy of that day's recorded stream replayed behind an opening frame-number
message made in the demo's layout (every message accepted, issues updated, heartbeats,
and ping replies that echo their pings). With the
demo's assets (`--demo-cache`, default `$BRISK_MOCK_CACHE`) it also loads the 2021 demo's
snapshot into SBI's decoder under 2021's hours and compares every quote with the demo
decoder's. Like the demo assets, these stay in your cache and out of Git. Needs Node
22.13 or later.

A live Next adapter still needs an authenticated Next session to establish its
actual bootstrap, master/snapshot, WebSocket handshake, keepalives and catch-up
behavior. The upstream native wire decoder remains experimental. Downloading
this demo, or hosting its vendor WASM decoder in Node, does not verify those live
transport details. The demo protocol version is 16000; upstream's SBI research
describes version 18000, which must not be substituted into this adapter blindly.

For immediate frame batches without Rust/file publication:

```sh
node briskapi/decoder/decoder.cjs --cache /tmp/brisk-mock-cache --speed 1
```

The Rust state is also available as the `brisk_quote_ingest::State` library for a
future signal engine. Feed ingestion is event driven. The optional latest JSON
file is published atomically every 20 ms when dirty (`--publish-ms` changes this);
this publication interval is independent of decoder ingestion. Use the streamed
batches or in-process state for immediate signals. The pipe applies backpressure
instead of dropping book updates. A slow consumer therefore creates lag, which
must be measured before production use.

## Data and timing

Each quote preserves `issue_id`, the complete master `code`, frame and source
time, plus bid/ask with the quantity and order count at each, indicative price and
matched volume, indicative opening price, market-buy/sell quantities, separate
closing-condition market quantities, special quote flags/time, the reference price,
traded volume and turnover (`turnover10`, tenths of a yen: price times shares, so
`turnover10 / volume / 10` is the VWAP in yen), and the issue status (0-7, the
decoder's; 1 is pre-open, 2 the morning session). `bid_levels` and `ask_levels` hold
each side's ten best prices that have orders, best first, as `[price10, quantity,
orders]`; `bid_closing_levels` and `ask_closing_levels` the same for the
closing-condition (引け条件付) limit orders. `bid_total_quantity`, `bid_total_orders`,
`ask_total_…` and `bid_closing_total_…`, `ask_closing_total_…` sum each group's limit
orders over the whole board. Prices suffixed `price10` are tenths of a yen; a zero price is the vendor's
unavailable sentinel. The indicative matching price excludes closing-condition
orders; closing indicative fields remain separate. `reference_price10` is the price
the decoder's indicative matching starts from (the special quote's price, otherwise
the last, otherwise the base price), not a second estimate of the opening price; in
continuous trading it equals the last price. `indicative_imbalance_side` and
`closing_indicative_imbalance_side` are 1 when the sell quantity at that indicative
price exceeds the buy quantity (or nothing matches), else 0. Buy/sell and quote enums
are preserved as raw vendor values; no unverified tradability or execution rule is
derived from them. Policy version 5 renamed `auction_reference_price10` and
`indicative_side` to these names, and added the book, turnover, the closing
imbalance side and SBI's issue status. Policy version 6 added the closing-condition books, the board totals and trades. An
issue whose quote the decoder can't read (its state for that issue is inconsistent) is
listed in the batch's `skipped_issues` instead, until it updates.

Bootstrap, history and quote batches carry `trades`: every execution the decoder recorded
since the previous batch (history batches: those its snapshot restored), by issue and in the decoder's order, as `{issue_id, code, frame,
time_us, price10, quantity, side}`. `quantity` is shares, `time_us` the execution's time
(the same clock as `source_time_us`) and `side` the decoder's raw type: 1 when the trade
was at or above the prevailing ask (buyer-initiated), 2 at or below the bid, 0 for most
opening auction prints. An issue's volume and turnover change by exactly its trades.

`source_time_us` is microseconds since JST midnight on `trading_date`, as BRiSK's
server stamps it: absolute in the snapshot, then moved by each update's delta. The
decoder has no clock of its own, and whether the server's stamps are exchange event
times is not established (see ARCHITECTURE.md).
`received_unix_ms` on stream batches is the local mock replay receipt time with
millisecond precision. `decode_ns` measures local decode plus quote extraction (with a live host's quote
workers, the host's decode plus the slowest worker's share).
Neither difference establishes live exchange latency.

The initial snapshot is at **2021-09-27 08:59:59.999955 JST**, with 4,131 securities
and 3,891 nonzero indicative prices. Toyota has price10 101500 (JPY 10,150), matched
volume 304,700, market buys 201,300 and market sells 105,700, with no trade yet.
The mock container has 18,001 frames labelled 09:00:00.000–09:02:59.990; the final
decoded market clock is 09:02:59.999896. Thus it provides **one genuine historical
pre-open snapshot and the opening transition**, not a pre-open time series.

All output is labelled `source=historical_mock`. `replay_running` means an
initialized replay is running, not that quotes are live. Completion, sequence
gaps, time/frame regressions, malformed input, child failure and interruption
invalidate that status. Consumers must also check `published_unix_ms` freshness
to detect a killed or stalled process; a quiet ticker's last update is separate
from stream health. Initial snapshot/frame-number catch-up is checked before
publishing. Restart loads a fresh snapshot; there is no automatic live reconnect.

## Latency indication

For strategies, consume the decoder's JSON batches directly (or `briskapi.Feed`)
rather than polling the latest-state file, which is published on a 20 ms timer.
The NautilusTrader v2 actor that used to live in this repository, with its
in-process Rust state, is maintained outside it. The standalone CLI described here
remains useful for recording and latest-state files.

The terminal prints local latency indicators once per second; `--status-ms 0`
disables this display. The latest-state JSON contains a `latency` object with
last/p50/p95/p99 values over the most recent 512 frame batches, plus session sample
counts and maxima:

- `decode`: local WASM decode and auction-field extraction.
- `local_receive_to_state`: Node mock receipt through decode, JSON serialization,
  pipe/backpressure, and Rust parsing/state application. Uses same-host wall
  clocks at millisecond receipt precision; negative readings are excluded and
  counted as clock anomalies. Clock synchronization/steps still affect readings.
- `replay_schedule_lateness`: monotonic scheduling/backpressure delay against
  the requested replay pace. Unpaced replay (`--speed 0`) leaves this unavailable.
- `exchange_delay_ms`: explicitly null, with a reason. This historical mock
  does not retain original exchange-to-client receipt times, and the origin of
  its source timestamp has not been established.

The original quote clock and trading date remain available. A source timestamp
of 08:59:59.999955 and a client arrival time of 09:00:00.020000 would indicate
20.045 ms age, not zero delay. For live data, receipt-minus-source becomes an
exchange latency estimate only after verifying TSE origin and local/source clock
alignment. Otherwise it must be labelled source-clock age. The browser bundle
exposes a separate server heartbeat clock; its extrapolation to the client does
not independently prove exchange timing. A quiet security's old update time
measures quote age, not network latency. The UI publication timestamp can also
be used to detect a stalled collector.

## Upstream recordings

Checked 7 October 2026:

- [`pybrisk` main at 43603e4](https://github.com/obichan117/pybrisk/tree/43603e443df4a412c1aa87cd1baca0848b8ccf1d)
  contains protocol research/mappings but excludes the raw binary capture.
- [`gh-pages` at ca4c77d](https://github.com/obichan117/pybrisk/tree/ca4c77d1553dfd4debdad3de41dbe77ef66dc104/research/ws_frames)
  **does contain raw recordings**: 30,602 binary frame files, master and decoder
  assets. Its [capture summary](https://raw.githubusercontent.com/obichan117/pybrisk/ca4c77d1553dfd4debdad3de41dbe77ef66dc104/research/ws_frames/capture_summary.json)
  lists 30,566 entries and creation time `2026-03-11 12:17:45`. The summary's 60
  received nine-byte `f0` pong payloads, interpreted as little-endian epoch
  nanoseconds, span **12:12:40.884003–12:17:35.884913 JST**. This agrees with
  lunchtime provenance and does not establish a morning pre-open recording.
- No GitHub releases are published. No morning pre-open stream was found in the
  published branches inspected. The site's 2021 mock is a separate dataset.

## Verification

Download assets first; fixture integration checks are opt-in to avoid a network
dependency in ordinary unit tests. Set the cache environment variable for the
complete suite:

```sh
BRISK_MOCK_CACHE=/tmp/brisk-mock-cache node --test --experimental-test-coverage tools/brisk_mock/*.test.cjs
BRISK_MOCK_CACHE=/tmp/brisk-mock-cache cargo test --manifest-path rust/brisk_quote_ingest/Cargo.toml
cargo clippy --manifest-path rust/brisk_quote_ingest/Cargo.toml --all-targets -- -D warnings
node tools/brisk_mock/benchmark.cjs /tmp/brisk-mock-cache
.venv/bin/python tools/brisk_mock/verify_browser.py --cache /tmp/brisk-mock-cache
```

`tools/brisk_mock/test_reference.py` (same cache variable) replays the demo and
checks that it matches the archive's committed reference fingerprint, which the
service uses to accept only genuine demo replays. Uploaded live broker recordings
(SBI, e-shiten) have separate structural and continuity checks without a reference replay.

The last check uses installed Playwright/Chromium and compares six auction fields
for six securities with the public demo's own JS accessors while its tutorial
keeps replay paused. It exposes the market object in an in-memory response; it
does not save or redistribute the vendor bundle. Complete replay asserts 731,251
quote updates across all 4,131 securities and verifies Toyota's opening trade.

The legacy demo ABI is not proof of the current live Next protocol. Production
requires a current account/session bootstrap, current master (including alphabetic
and class-share identity), measured feed continuity/latency, and validation against
the current live books. This implementation has no order transport and does not
change the paper runner or dashboard.
