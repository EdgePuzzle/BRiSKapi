# Architecture

How briskapi works, what the data means and how the shared archive protects
itself. For usage, see the [README](README.md).

## Components

| Component | Where | Role |
| --- | --- | --- |
| Decoder hosts | `briskapi/decoder/` | Run BRiSK's own WASM decoder in Node, without a browser or Chrome DevTools. `decoder.cjs` replays the public demo, whose assets must match SHA-256 pins in `assets.json`. `sbi.cjs` drives SBI BRiSK's (or e-shiten BRiSK Next's) decoder from an authenticated session. Both emit one JSON batch per frame. |
| Python API | `briskapi/` | `Recording` (files), `Feed` (live, background thread), `Ticker`/`Market` views and `Archive`. |
| SBI session | `briskapi/sbi.py` | Cookie login (SBI, or e-shiten with `site="e-shiten"`), token boot and the REST data behind `Ticker.candles`/`margin` and `Market.turnover`/`lists`/`events`/`schedule`/`watchlist`; `sbi.connect()` starts the live host. |
| Archive client | `briskapi/cli.py`, `briskapi/schema.py`, `briskapi/_parquet.py` | The `brisk` CLI, sharing consent, Parquet packaging, upload, verified download and export. |
| Auction state | `rust/brisk_quote_ingest` | Optional Rust tools: `brisk_quote_ingest` validates batches, keeps per-security state and publishes latest-state files; `brisk_recording` reconstructs a saved recording. |
| Archive service | `archive_service.py`, `infra/deploy.py` | A Lambda function URL issues upload tickets; S3-triggered ingest validates and publishes; DynamoDB holds quotas. |

```
demo assets (HTTPS, pinned) ─► decoder.cjs ─┐
SBI/e-shiten session ────────► sbi.cjs ─────┴─► JSON batches ─┬─► briskapi.Feed: live state, callbacks, iterators
                                                             ├─► brisk live: JSON lines
                                                             └─► briskapi.record ─► Parquet recording (demo only)

JSON batches ─► Parquet tables ─► zip ─► upload ticket ─► S3 staging ─► verification ─► archive/ ─► briskapi.pull
```

## Data source

The account-free input is the public BRiSK Next demo dated 2021-09-27. Its
initial snapshot is at 08:59:59.999955 JST with 4,131 securities. The frame
container has 18,001 frames labelled 09:00:00.000–09:02:59.990, so it provides
one genuine pre-open snapshot and the opening transition, not a pre-open time
series or a full-depth book. Output is labelled `source=historical_mock`.

Source clocks are preserved. They are the stamps BRiSK's server sends; the decoder
reads no clock of its own (its output is byte-identical under another time zone and
system date). Each issue's snapshot record carries an absolute time in microseconds
after JST midnight, and each later update a delta from that issue's previous update,
which the decoder adds up; a catch-up reply carries deltas only, and an issue with no
snapshot starts at 07:00. About once a minute a time message (microseconds since
07:00 JST) moves the stream clock, `time()`, which is the latest of those ticks and the
update times. Whether the server's stamps are TSE event times passed through or its
own is not established. Its own looks likelier: an issue's updates are never stamped
less than about 0.3 ms apart, mostly 0.4-0.45 ms, whatever the load and in both 2021
and 2026, like a fixed per-issue processing step; the 09:00 status changes are stamped
61-274 ms after 09:00, each on its own; and an update stamped exactly at the session's
close ends it, so some stamps are conventions. Against that, they have true microsecond
precision, are slightly out of order across issues, and a sweep's trades can share
one. A live session with a snapshot, comparing update times with the server clock in
its heartbeats, could settle it. Until then exchange provenance and exchange delay are
unverified: `exchange_delay_ms` is always null, and `source_timestamp_origin` is
`brisk_decoder_unverified`, a historical name for "as the decoder reports the server's
stamps". `special_quote_time_us` is an issue's own update time plus 180 s, the same
clock. Live data needs an
authenticated Tachibana BRiSK Next session and validation of the current
protocol: bootstrap, master/snapshot, WebSocket handshake, keepalives and
catch-up. The demo's protocol version is 16000. The [recorder
documentation](tools/brisk_mock/README.md) compares this with pybrisk's
browser-based live reader and defines every field and timing measurement.

## Batch stream

Every source produces the same stream:

1. `bootstrap`: the security master and every selected security's initial quote,
   plus transport and clock provenance.
2. `quotes`: one batch per frame with the quotes that changed in it. Empty
   batches are kept, so sequence continuity and feed health stay separate from a
   quiet security's last update.
3. `end`: frame and update counts, written only after a clean finish.

Sequence gaps, clock regressions, malformed input and interruption invalidate a
stream in the Rust state and in archive validation. The Python feed checks
sequence continuity and a clean end.

Recording time queries filter each quote by its own `source_time_us`, including
quotes in batches whose clock is later than the query. Batch clocks are upper
bounds on contained quote times, so stopping at the first later batch would miss
valid snapshots and deltas. `Market.summary()` uses the first and last batch
clocks, including empty batches and the end marker; recordings cache that range
when no manifest is available, while feeds track it under the state lock.

## Recording format

A recording is a directory of three Parquet tables (`briskapi/_parquet.py`), laid out
like a NautilusTrader v2 catalog's `data/`. Each table is v2's own schema with BRiSK's
fields added after it. Rows carry v2's `identifier` column (`CODE.XTKS`), as v2
returns multi-instrument queries, instead of one directory per instrument: a
directory per security would make the complete demo 8,262 small files and about three
times the size. Reading gives back the decoder's batches exactly, so validation and
reference fingerprints are the same for every format.

| File | Rows | Columns |
| --- | --- | --- |
| `data/order_book_depths.parquet` | One per quote update, sorted by issue, then batch | v2 `OrderBookDepth`: `bids`, `asks` (up to ten levels each, best first: `price`, `size`, `count`, `order_id` 0), `flags` (16, plus 32 on the bootstrap snapshot), `sequence` (the frame number), `ts_event` (source time), `ts_init` (the same). Then `identifier`, `issue_id`, `seq` (the batch), `max_frame`, the prices (`last_price`, `open_price`, `bid_price`, `ask_price`, `indicative_price`, `closing_indicative_price`, `indicative_open_price`, `reference_price`; null for the vendor's zero), the counts and enums (`indicative_volume`, `indicative_imbalance_side`, `closing_indicative_volume`, `closing_indicative_imbalance_side`, `quote_flag`, `quote_side`, `volume`, `issue_status`, market and closing market buy/sell quantities, `bid_quantity`, `bid_orders`, `ask_quantity`, `ask_orders`), `special_quote_time` and `turnover` |
| `data/equity.parquet` | One per security, by issue | v2 `Equity` (`id`, `raw_symbol`, `currency` JPY, `price_precision` 1, `price_increment` 0.1, `lot_size`, `max_price`/`min_price` from the daily limits, `ts_event`/`ts_init` at JST midnight; `info`, JSON in v2, is a null string), then `issue_id`, `tick_type`, `issue_type`, `name`, `base_price` |
| `data/custom/BriskBatch.parquet` | One per batch | `seq`, `kind` (bootstrap, quotes or end), `received_unix_ms`, `decode_ns`, `replay_lateness_ms`, `skipped_issues`, `frames`, `quote_updates`, `replay_wall_ms`; on the bootstrap row `source`, `trading_date`, `input_transport` (canonical JSON), `source_timestamp_origin`, `exchange_delay_ms`, `market_issue_count`; v2 custom data's `type`, `ts_event` and `ts_init` |

Prices and level sizes are `decimal128(38, 16)` as in v2; times are UTC
`timestamp[ns]`. `ts_init` repeats the source time rather than a local clock, since a
demo replay's receipt clock is years after its market time; receipt times stay exact
in `BriskBatch.received_unix_ms`. Tables are zstd-compressed in row groups of 65,536
rows, so one security's history reads only its row groups. The complete demo replay
(735,382 updates) is 34 MB, against 754 MB of JSONL and 117 MB gzipped.

`brisk export DIR --nautilus CATALOG` writes v2's layout: `equity` and
`order_book_depths` per instrument, readable by v2's `ParquetDataCatalog`, and the
rest of each quote as custom data `BriskQuote` (int64 columns in vendor units, so a
v2 `customdataclass` can declare them). `--jsonl FILE` writes canonical batches.

## Live feed

`briskapi.Feed` reads decoder output on a background thread and applies each batch
to its state under a lock. A new listener takes its snapshot and registers in one
locked step, so it can't miss or duplicate an update. Iterators use a bounded
queue (10,000 items) and callbacks run on the feed thread, so a slow consumer
slows the decoder instead of dropping updates. An abandoned iterator never blocks
the feed. An exception in a callback stops the feed and is re-raised by `wait()`.
A feed keeps current state only, unless `history=True` retains every update per
security.

When sharing is enabled, a demo feed also writes the session to a temporary file.
After a clean end it packages and uploads the file, then deletes it. SBI feeds
accumulate timing statistics when sharing is enabled (`briskapi/timing.py`). Market
recording additionally requires a separate opt-in for that session. The interactive
CLI asks before starting the decoder (Enter accepts); scripts use
`--share-market-data` or `sbi.connect(share_market_data=True)`. The per-session
choice is not persisted. Current consent is required, and `BRISK_CONTRIBUTE=0`
or `contribute=False` disables both forms of sharing. Only a clean end publishes
market data; an interrupted capture's temporary file is deleted.

## Timing reports

A timing report is shared independently of a broker (SBI, e-shiten) market-recording opt-in. A report
(`brisk-timing-v1`) holds p50/p90/p99/max of decode time, feed-clock age at
receipt and frame spacing, a stall count (gaps over one second), the frame
count, trading date, first/last JST minute, client version, alias and license.

The ticket API accepts `{"timing": report}`. The service checks exact fields,
bounds and microsecond precision, monotonic percentiles, at least 100 frames and
a trading date within the last 30 days; rate limits it (six per IP address per
hour, 600 in total); and writes its own canonical JSON to
`timing/YYYYMMDD/SHA256.json`, so identical reports deduplicate. A report is at
most 2 KB, so it can't carry other data. `briskapi.Archive().timing()` reads
them back.

## SBI BRiSK

The endpoint sequence was learned from pybrisk: session cookies authenticate
`/api/frontend/boot`, which returns a bearer token; `/api/app/boot` then gives
the trading date, series, schedule, WebSocket URL and master/snapshot hashes.
Requests are rate limited and never follow redirects, because a redirect means
an expired session and following it would forward credentials. `sbi.connect()`
asks `/api/frontend/boot` before it starts the live host, so an expired session
is one `SessionExpiredError` line and Node never starts. Cookies given as JSON
(the `BRISK_*_COOKIES` variables or a saved cookie file) that don't parse are
the same error, naming the variable or file but never the value.

The live host (`briskapi/decoder/sbi.cjs`) uses the same session to fetch the
master and snapshot, finds SBI's decoder through the app's own bundles (it is
served only to logged-in sessions and changes with SBI releases, so it can't be
pinned), checks that it exports every function the host calls, and decodes
WebSocket frames with protocol version 18000. Cookies reach it through the
environment, never the command line.

The page may name its bundles by absolute or relative path, and a bundle names the
loader either as a literal path or as a template with an exchange variant,
`./assets/wasm${r}/fita.<hash>.js` (`r` is `-fukuoka` for the Fukuoka exchange; only
the Tokyo build is used), in which case the `.wasm` name follows the loader's, as
Emscripten names them. Bundles on another host are never fetched. The boot's
`ws_url` is relative: as in the browser, it is resolved against the frontend boot's
`api_endpoint` (`https://api.brisk.jp` on e-shiten and in pybrisk's SBI sample), or
the site when there is none. A stream outside `brisk.jp` is refused. Cookies go only
to the site's own host; elsewhere the session in `ws_url` authenticates the stream,
which is opened with the site as `Origin`.

The host follows the connect sequence in the vendor client (the public Next demo
bundle) and in pybrisk's SBI research (`engineio.cjs` and `sbi.cjs`):

1. The WASM is started with its six callbacks (`send`, `updateNumber`,
   `heartbeat`, `basePrice`, `authError`, `marketFinished`) using the vendor's
   signatures (plus the session schedule on the 2026 builds, above). `send` bytes are
   the WASM's replies to the server's pings, written to the socket as raw binary
   frames: in pybrisk's recording of SBI's stream the server sends a 9-byte ping
   every 5 seconds and the browser echoes each one back unchanged. The vendor
   client has no ping timer of its own.
2. The link detects the server's dialect from its first message. The Next client
   uses a plain binary WebSocket, and SBI's stream is recorded as one too. It is
   `/realtime/0` on the API host the frontend boot names (`api.brisk.jp`); pybrisk's notes
   give `sbi.brisk.jp`, but the boot in the same capture already named `api.brisk.jp`, and
   the site host now answers `/realtime/0` with 404. The host asks for the WebSocket
   subprotocol `flex`, as the vendor client does (`new WebSocket(url, "flex")`). If a server opens with an Engine.IO packet instead, the host
   falls back to joining a Socket.IO namespace (`/v2/user`) with a query, sending a
   `startLive` event and answering Engine.IO keepalives (clients ping on version 3,
   servers on version 4). That fallback comes from a separate Socket.IO connection in
   the same capture, which upstream's notes took for the stream's wrapper: its frames
   are numbered apart from the binary ones, its `url` parameter names pages of SBI's
   own website, and its keys don't have the shape of BRiSK's values. It most likely
   belongs to another service on SBI's site, so its parameters are unverified.
3. Frames go to the WASM immediately. When the first frame numbers arrive, the
   host compares them with the snapshot's. Issues whose snapshot is behind are
   caught up with `POST /api/stocks_update/{series}` (up to five retries, never for
   an expired session or an `x-error-reason: too-old` reply) and the reply is
   pushed into the WASM before quote tracing starts. Frames that arrive meanwhile
   are not lost: the decoder holds them until `_apiRecieved`, then applies them,
   skipping updates the snapshot already has. The decoder applies whatever reply it
   is given, so afterwards each caught-up issue must be exactly at the stream's first
   frame and every other issue where it was; a reply pushed twice, or a repeated or
   stray update, stops the session (on SBI's March data the real reply passes and a
   second push of it is refused).
4. The server heartbeat, decoded by the WASM, is watched: no beat for 10 s after the
   first one is a connection failure, as in the vendor client. A second rule is the
   vendor's too: during trading hours (08:00-15:00 JST, not 11:30-12:10) the
   decoder's data time may lag the server clock, taken from the heartbeat, by at
   most 90 s. The WASM's `authError` is the single-session rule (one WebSocket per
   user), and `marketFinished` makes an abnormal close a normal end.
5. The decoder protocol comes from the boot response's `flex_version` (an explicit
   option wins; 18000 if the server says nothing). The master is checked as soon as
   it is read (below). The boot response's `exceptional_sq` and `base_prices` are
   pushed into the WASM after that and before the snapshot, as the vendor client does, and the master's
   limits are re-read after every frame in case the decoder changes them (a pushed base price
   moves the record's limits; in both builds its base price field keeps the loaded value). A special
   quote names its issue differently by build: the demo's `_pushSQ` takes the security code as a
   number and looks it up, while the 2026 builds take the issue index and read the code (`167A`
   too) from their own record. Given a code, a 2026 build files the quote under whichever issue
   has that index (1301's under 8283), or rejects it, which it reports only on stderr ("Invalid
   Issue Code"). The host sends the index at 18000 and the code at 16000. A special quote whose
   code is not in the master is not sent and is listed in `boot_applied.skipped_exceptional_sq`.
   If the decoder still rejects one, the session stops: the build would be naming issues in yet
   another way. On SBI's March build, boot special quotes for 167A, 1301, 25935 and 7203 each land
   on their own issue and no other (`tools/brisk_mock/sbi_master_layout.py`).

The ping loop is the WASM's own. In the real decoder, market and sync frames never
trigger a ping; each heartbeat frame (`0xF0`, `0xF2`, `0xF4` or `0xF6` and an 8-byte
little-endian clock in nanoseconds) triggers one reply: the same nine bytes, echoed.
The loop therefore sustains itself once the server starts it, and the host only has to
forward the bytes.

The decoder reads its input as one byte stream, so frame boundaries don't matter to it;
`_pushWs` returns nothing, but writes the latest source time of the updates it applied
and how many it applied (none for pings, clock messages and frames held before
`_apiRecieved`). After `begin()`, every frame of stock updates (odd first byte) applies
at least one, through the demo's recorded day and SBI's recording. One that applies none
leaves its bytes held for the next frame, and if the frames after it apply none either,
the parser has lost the messages' boundaries: it decodes nothing more, raises no error,
calls nothing back and answers no ping. The host then stops the session at once, naming
the frame after which it happened, rather than waiting for the heartbeat rule. Decoder
failures are made readable too: a parse error (an unknown message header, an HTML page
pushed as data) escapes the WASM as a bare number, the address of a C++ exception whose
message the host reads, and a WebAssembly trap leaves the decoder's stack 16 bytes down,
which the host restores; either way the error names the frame or the issue.

The stock view and the master are read at version-specific positions. Protocol
16000 (the demo) is the vendor client's own layout. For 18000 (SBI) the stock
view has the same fields in the same order with the leading word, the numeric
security code, dropped (the code is text there), so every index is one lower.
SBI's decoder of March 2026 confirms it: its `_getStockView` writes the demo's fields
in the demo's order, and with the demo's snapshot loaded (below) every word of all
4,131 issues' views equals the demo decoder's word one place on. Any other protocol
version is refused rather than read at guessed offsets.

What some recorded fields hold, read from both builds and checked on the demo's data
(the values stay the vendor's raw ones). `quote_flag` 2 is a special quote, the only flag
that starts the decoder's 180 s timer (`special_quote_time_us` is its start plus 180 s);
3 is priced like it without the timer; 1, a mark with no effect on prices, occurs only at
16000 (SBI's build drops it, so the same frames give 0 there). `quote_side` is 0 for the
buy side and 1 for the sell side, and means nothing when the flag is 0.
`reference_price10` (named `auction_reference_price10` before policy version 5) is the
price the decoder's indicative matching starts from:
the special quote's price while there is one, otherwise the last price, otherwise the base
price (at 18000 a price the server sends with the issue's status comes first, and after
the morning close the morning's last price holds until the afternoon trades). In
continuous trading it therefore equals `last_price10`. `indicative_imbalance_side`
(formerly `indicative_side`), and `closing_indicative_imbalance_side` for the closing
auction, are 1 when the sell quantity at that indicative price exceeds the buy quantity
(or nothing matches), else 0. `turnover10` is price times shares in tenths of a yen; its
bars' turnovers sum to it and its VWAP lies between the day's low and high on all 3,774
traded demo issues. `issue_status` takes the same values in both builds (1 pre-open, 2
the morning session, 5 the afternoon's; 3, 4, 6 and 7 are the lunch break, the
afternoon pre-open, the closing auction and the close as far as the code shows): the
demo's from `_getPortfolio`, SBI's from its view, less 8.

The book comes from `_getItaRows(id, issue, first row, rows, price rows, market row, OVER,
UNDER, light)`, which writes 204-byte rows: price (tenths of a yen) at byte 0, then buy,
closing-buy, sell and closing-sell groups of quantity (u64) and order count (u64, 16 bytes
on) at 20, 64, 112 and 156. Row indices step one tick from the lower limit to the upper
(`_getTickPrice10ToIndex(id, tick type, price)`; the tick type is not the issue); OVER and
UNDER sum every row above and below the window read, and the market row holds the market
orders and, in its closing groups, the market-on-close orders. Its layout is the same in
both builds, which return byte-identical books for every issue of the demo's snapshot. A
quote keeps each side's ten best prices that hold orders, and the quantity and order count
at its own bid and ask: the host reads 32 rows from the quote's bid (or ask) toward worse
prices, and on until ten are found or nothing lies beyond; when OVER (UNDER for sells)
shows orders at better prices, as in crossed auction books, it reads every row beyond
first, in one call, since each call sums the whole board. On the demo's data this equals
reading the whole board on every issue at several times, in both builds, and costs about
22 µs per quote (a quote about 47 µs in all; boards run to 3,521 rows).

An issue whose quote traps the decoder (its state for that issue is inconsistent: see
below) is skipped, listed in the batch's `skipped_issues`, and not read again until its
frame number moves, because read again at the same frame the decoder returns stale
derived fields instead of trapping. A bootstrap's quotes and skipped issues together cover
its master; a skipped issue is quoted once it updates and reads.

The master is read from the decoder's own records (`_getStockMaster`), as the vendor
client does, at the version's layout (`MASTER_LAYOUT` in `decoder.cjs`). 16000's is the
vendor client's: 144 bytes, the code a number. In 18000 the code is eight bytes of text,
which must hold codes such as `167A` (350 of 4,445 in SBI's master of 2026-03-11), and
every later field is four bytes on: 148 bytes. That layout was found by running SBI's
decoder of March 2026 and the master file it served, both from pybrisk's research, and
matching the records against the file: all 4,445 agree on code, base price, limits, lot
size and issue type, and the names and tick types (0, 1 and 3) are the decoder's own.
`tools/brisk_mock/sbi_master_layout.py` repeats this from pinned downloads.

The master file the decoder loads checks every master read (`checkMasterFile`). It holds
fixed 180-byte records in issue order; pybrisk documents the record size, the binary header
(a word of unknown use, then base price, upper and lower limit in tenths of a yen) and the
code's position. The 2021 demo's file has the same records after a 444-byte preamble, and
reading it equals the demo decoder's own master on all 4,131 issues, which places issue
type at byte 40, the code at 50 (five wide, for some preferred shares) and the lot size
right-aligned up to 139. Records are counted back from the end with the decoder's issue
count. The tick type is bits 25-26 of the record's first word, whose low byte is the
master record's message tag (`0x18`); it equals the decoder's on all 4,131 demo and
4,445 SBI issues. If any issue disagrees on those seven fields, the session stops: a build whose
record layout has changed, or a file that does not line up with the decoder's issues,
can't pair quotes with the wrong codes. Names are not compared, because the file's
differ (the decoder keeps `Ｕ－ＮＥＸＴ` whole but drops a market prefix such as
`Ｍ－`). The master is also refused if it
does not look like a market (security codes, base prices inside their limits, plausible
lot sizes, names).

The 2026 decoders (SBI's of March 2026, and the build both sites served from October)
take an eighth `_initialize` argument that the demo's doesn't: the session schedule,
made by `_createSchedule` from the boot's `schedule_info` (seven session times in
microseconds after midnight, then `sq_jump_interval`, in the boot's order). The host
passes it whenever the decoder exports `_createSchedule`; the demo's build starts as
before. The boot gives `sq_jump_interval` in seconds (180), but the decoder adds it to a
special quote's time in microseconds, where the demo's build has a fixed 180,000,000, so
the host passes it in microseconds and refuses a value outside 1-3,600 s. Passed as 180,
a special quote's next time came 180 µs after the quote instead of three minutes (no
broker session had streamed yet, so no recording carries such times). In microseconds, the first 3,000 frames of the
demo's stream replayed through the March build under 2021's hours give the demo decoder's
special quote time on all 141,281 updates (4,738 of them with one). `_createSchedule` stores the eight values as integers and counts the bars as
(morning close - open) / 5 min + (afternoon close - open) / 5 min; every stock keeps a
pointer to the schedule, and a trade's time is turned into its bar through it. Without
a schedule these decoders keep no price bars (`_ohlcLength` is 0), can't return a stock
view, and fail on a snapshot with `deque` (fita.cc:1133). A snapshot's bars must match
the schedule's count: anything else is refused with `Invalid unserialize`. The demo's
snapshot (60 bars: TSE closed at 15:00 in 2021) loads into both 2026 builds only under
2021's hours, and then every one of its 4,131 quotes equals the demo decoder's, field
for field; under 2026's hours (66 bars) it is refused.

The catch-up request is the vendor client's, as pybrisk's capture recorded it:
`POST /api/stocks_update/<series>?date=<date>` with the JSON object
`{"issue_ids": [...], "update_numbers_from": [...], "update_numbers_to": [...]}`, one
entry per issue whose snapshot is behind the stream. The binary reply is pushed into
the decoder as it comes.

What is verified: the decoder SBI served in pybrisk's March 2026 capture exports
the demo's interface apart from `_getPortfolio`, and it initializes under Node with
protocol 18000. (Its stock view carries the issue status one word after the demo's
last, as the status plus 8; minus 8 it equals the demo's `_getPortfolio` status on all
731,251 views of the demo's stream replayed through both builds. SBI quotes record it
from there.) Offline, it loads
that capture's master, schedule and catch-up reply: the catch-up advances each of its
1,552 issues by exactly the updates requested, briskapi's request for them is the one the
vendor client sent, and 1,549 stock views read back the decoder's frame numbers; started
without the schedule, none does. The other three trap: with no snapshot under the
catch-up, an update takes an order quantity below zero, and the decoder traps converting
it to an unsigned integer (`float unrepresentable in integer range`). Full quotes, which
also read the order book, trap on 292 of the 1,552. With the demo's snapshot under it, no
view or quote traps; a live session reaches this only with a missing snapshot or a lost,
repeated or reordered update, and the issue is then skipped (above). Given the demo's assets, the same tool
also loads the demo's snapshot as above (`tools/brisk_mock/sbi_master_layout.py`). The
October build did the same in a one-off check. The demo
decoder is tested against the vendor client's own indices on real data, and its
heartbeat, ping reply and callbacks through its function table; the ping loop and
the whole flow run offline against fake servers (plain, Engine.IO 3 and 4) using it.
Replayed through SBI's March decoder, the archive's copy of pybrisk's SBI stream
(2026-03-11, the `sample_data` wire capture: 30,505 messages from 12:12:40 JST) starts
partway into the connection, without the opening frame-number message, so the decoder
applies no stock update from it on its own. Given one made in the demo's layout (`0x88`,
a 16-bit issue count, a 32-bit update number per issue, here all 0; it is the only
opening message either build knows), the recording stalls the decoder: every 40 s, after
every eighth ping, it holds the same 15-byte frame (`99eb2c6a…`), from which the decoder
can't complete a message. It misreads the frames after it (its clock jumps ten hours;
in the demo's build that frame takes 5 s), then finds no message boundary again, so it
answered only the first eight of the 60 pings and applied 14,160 quote changes. The host
now stops there and names the frame. Without those seven frames, every message is taken,
all 60 pings are answered with themselves, as the browser's were, and 84,160 quote changes
apply to 3,279 issues (values relative to the missing snapshot: Toyota's bid and ask read
0). The browser's decoder, the same build, answered all 60 pings, so it was probably not
given those frames as recorded: its client may drop them, they may come from another
connection, or the capture may have damaged them. A live session will tell. The offline
check (`tools/brisk_mock/sbi_master_layout.py`) replays the stream both ways.
(pybrisk's own `capture_summary.json` has wrong bytes in 2,478 of these messages; the
archive's copy matches pybrisk's per-message files.)
What is not verified against a live session: any of the above on SBI itself, the
18000 master layout on today's build (found on SBI's of March 2026; each start checks it
against the master file), the stream's opening messages and whether it is plain binary
as recorded. Live, the master and the snapshot load with the schedule; the WebSocket
upgrade to `api.brisk.jp` was then answered with HTTP 502 (400 without the `session`
parameter, 404 on the site host, and no HTTP/2 extended CONNECT) with every header
variant tried, all without the `flex` subprotocol, which the host now asks for; whether
that is all the server wants is not known yet. The Socket.IO fallback's parameters live in a
profile (`BRISK_SBI_PROFILE`, or `sbi.connect(profile=...)`) and are unverified. The
profile can also name another catch-up format (`catchUp`).
`--trace-protocol` prints the connection with every token redacted, so a first
attempt shows what the server answers. Anything wrong ends in an explicit error (a
stream that never initializes, a refused namespace, a rejected catch-up, an
implausible stock view or master) rather than guessed data. Not implemented: reconnecting.
The feed fails and needs a restart.

### Tachibana e-shiten BRiSK Next

e-shiten's BRiSK Next (`next.e-shiten.brisk.jp`) is the same app. In sessions on
2026-10-08, the two sites' `/api/app/boot` responses matched (`flex_version`
18000, the same master and snapshot hashes) and they served the same decoder
(`fita.35e38ad5…`), reached only through the template path above. On both, cookie
sign-in, every REST endpoint and the decoder, master and snapshot downloads worked.
Both stopped while reading the master with the demo's 144-byte records (a lot size
beyond 2^53), so the WebSocket was never reached: the decoder's 18000 master records are
not the demo's (above: 148 bytes; read at 144, the records drift until a lot size is
garbage). Their master file was about 800 KB for 4,443 issues, which fits the same
180-byte file records.
e-shiten enforces one BRiSK session per user: opening
it in a second tab stopped the first (message 10011).

`site="e-shiten"` selects it (`BRISK_SBI_SITE` for the host; saved cookies in
`e-shiten-cookies.json`, `BRISK_ESHITEN_COOKIES`). Its batches say
`source=eshiten_live` and `kind=eshiten_websocket`. Its sessions are contributed as
SBI's are: a timing summary under saved consent, market data only after the per-capture
opt-in, with the same opt-outs; packaging keeps only `next.e-shiten.brisk.jp` as its
transport provenance (`BROKER_TRANSPORT` in `schema.py`). `brisk login` can sign in to it with a passkey (site `e-shiten`; see
[Tachibana e-shiten's sign-in](#tachibana-e-shitens-sign-in)).

## Passkey sign-in

`briskapi.passkey` (CLI: `brisk sites | enroll | login | forget`) gets a broker's BRiSK session cookies by
signing in with a passkey, in place of copying them from a browser. It is not tied to any one broker:
a *site* (`briskapi/sites.py`) says where the login page is, what its passkey button says and which host
the BRiSK cookies belong to. SBI, Matsui, Monex and SMBC Nikko are built in (each runs BRiSK on its own
`<broker>.brisk.jp`, found by checking the hosts and the brokers' own pages; none is verified against a
live account), and so is Tachibana e-shiten (BRiSK Next on `next.e-shiten.brisk.jp`, its sign-in mapped
from its public login page). `~/.config/brisk/sites.json` adds or edits entries. A person normally has one
brokerage, so the saved login is a single record, `{site, credential}`, and `login` needs no site argument.
A site with a `user_field` (e-shiten) also keeps the `user_id` that `enroll` asked for.

### How a passkey sign-in works, and why Chrome can stand in

A passkey is a WebAuthn credential: a key pair made for one site (its *relying party id*, the site's
domain). Registration makes the authenticator create the pair and hand the public key to the site.
Signing in goes like this: the site sends a random challenge; the browser passes it, with the page's
origin, to an authenticator; the authenticator checks that the origin matches the credential's relying
party id, confirms the user (presence, and verification such as a fingerprint) and signs the challenge,
the origin and a counter; the site checks the signature against the stored public key, the flags and
that the counter did not go backwards. Nothing in that exchange says what kind of authenticator signed.
A site can only learn that from an *attestation* statement made at registration, and browsers and
sites mostly use "none" for passkeys. So an authenticator that is software, and that reports presence
and verification as done, produces a sign-in the site cannot tell from a phone's. Chrome's DevTools
`WebAuthn` domain provides exactly that (a *virtual authenticator*), including exporting and importing
a credential, which is how the passkey is kept between runs. Two things limit it: the site may detect
the automated browser by other means, and the passkey is now a secret that sits in software.
Passless-style virtual USB devices were ruled out: they need `/dev/uhid` (Linux only), and on macOS
creating a virtual HID device needs an entitlement that Apple grants on request. The Chrome route
needs neither and runs wherever Chrome and Node do: macOS, Windows and Linux. On Windows it is run from
Windows itself, using Windows' Chrome, Node and Python; WSL is Linux and would need its own Chrome.

### The helper

`decoder/passkey.cjs` runs under Node and drives a Chrome it starts itself. It holds no site of its own:
the login URL, button text, cookie host and any user ID box and user ID come in the request.

1. **Private pipe, no port.** Chrome is started with `--remote-debugging-pipe` and talked to
   over file descriptors 3 and 4 (NUL-framed JSON). A loopback debugging port would let any
   local process attach and read the passkey back out with `WebAuthn.getCredentials`. Windows
   passes those two descriptors to Chrome too (Chrome has a separate named-pipe switch for
   Windows, which is not needed): checked with native Node 25.8 and Chrome 154 on Windows 11.
2. **Ephemeral profile.** The profile is a new private directory, deleted when Chrome exits
   (`--profile-dir` opts into a persistent one). It holds the broker's logged-in session, so a helper
   that is force-killed (which cannot clean up after itself) must not leave it around: each profile
   records its owner's process id, and every start removes `brisk-passkey-*` profiles whose owner is
   gone, leaving live and just-created ones alone. The mock keychain and basic password store
   keep Chrome from prompting for Keychain access. Chrome reports `navigator.webdriver` as
   true whenever DevTools is attached (checked in both modes), and headless Chrome's user
   agent says so too, so a site that blocks automated browsers can tell. The host does not
   hide this. The default is a visible window, which lets a person take over where a guess
   about a broker's pages is wrong or the broker refuses the automation.
3. **A virtual authenticator on every page**, including popups and new tabs
   (`Target.setAutoAttach`, with new pages held until it is in place). It is a CTAP2.1 platform
   authenticator with resident keys that approves presence and verification itself. A page gets
   the latest copy of the passkey, so a popup that opens after a sign-in in the same run signs
   with the advanced counter rather than the one Chrome started with, which the site would
   refuse as a clone.
4. **Enrolling** attaches an empty authenticator, opens the site's `enroll_url` (or its login page) and waits for
   `WebAuthn.credentialAdded` while the person registers a passkey. The captured credential is
   kept in memory and saved, with the chosen site, only after the person confirms, so a failed
   enrollment never replaces a passkey that still works. `enroll` refuses to overwrite without
   `--replace`, and with no `--site` it asks which broker (or fails, without a terminal).
5. **Signing in** imports the saved credential, opens the login page, types the saved user ID into
   the site's `user_field` when it has one (`Input.insertText`, falling back to the value setter and
   `input`/`change` events; without the box nothing is pressed), clicks the control whose
   text contains the site's `passkey_button` (trusted `Input` mouse events; the shortest matching
   label wins, so a "can't sign in?" link does not; a control drawn as an image is labelled by its
   `alt`) and waits for `credentialAsserted` from any page, so a ceremony in a popup counts. Then it
   opens `launch_url` when there is one, or waits for the person to open BRiSK, and polls for a
   cookie that starts with the site's `cookie_prefix` on exactly its BRiSK host. Only that host's
   cookies are returned, never those of the broker's main site, whose session is destroyed with
   the profile.
6. **Delivery.** SBI and e-shiten have a data client (`briskapi.sbi`, with `site="e-shiten"` for the
   latter), so their cookies go to `sbi.login()` and are saved as that site's cookies; the
   others are saved to `~/.config/brisk/cookies/<site>.json` (owner-only) for the person's own use.
   A data client sends its cookies to one host, so a site bound to a client whose cookie host is not
   that client's host is refused before Chrome starts (a `sites.json` entry can never claim a client).

The sign counter is the delicate part. A relying party may reject an assertion whose counter
does not exceed the last it saw, as a sign of a cloned authenticator, and every sign-in
advances it. The host therefore streams the credential as JSON lines after every change, the
Python side saves each one at once, and a failed or interrupted run still reports the
latest. If a save fails, the helper is stopped rather than allowed to sign in again with a
counter that would no longer match.

Secrets move only over stdin and stdout, never on a command line or in a log message (logs
name origins, never paths or queries, and the helper refuses to run attached to a terminal).
The saved login is kept in the macOS Keychain through `security -i`, which reads its command from
stdin: `add-generic-password -w SECRET` would show the secret in the process list. `security`
exits 0 even when a write fails, so each write is read back and compared. On Windows it is one
Credential Manager entry for this user on this machine (`CredWriteW` and friends through `ctypes`, no
extra dependency; an entry holds at most 2,560 bytes, which a passkey fits in), also read back after
each write. Elsewhere it is stored only if `BRISK_PASSKEY_STORE=file` asks for an owner-only
file, which is weaker (and on Windows a file's privacy comes from its folder's ACL, since `0600` means
nothing there). The private key is the whole credential: whoever has it can sign in.

Three things differ on Windows, and the code and tests cover each. A process cannot be signalled to stop
there (terminating one kills it outright, which would leave Chrome and its profile behind), so the
helper treats a closed stdin as the request to close Chrome and exit, on every platform; the Python side
closes it first and terminates only if the helper does not exit within `GRACE` (20) seconds, which is
longer than closing Chrome and deleting its profile can take. (Killing the helper outright does make
Chrome exit on its own within about a second on Windows, because its pipe closes.) Text between
Python and Node is always UTF-8, because Python's default there is the system code page (cp1252) and a
Japanese user name or button text would fail to decode; `sites.json` is read as UTF-8 and also accepts
the byte-order mark and the UTF-16 that Windows editors and PowerShell 5.1 write. And deleting the temporary profile retries,
because Windows can keep a file locked for a moment after Chrome exits.

Tests run Chrome against a fake broker (`tools/brisk_mock/fake_sbi_passkey.cjs`) that verifies
what a real site verifies: the ES256 signature, the relying-party hash, user presence and
verification, and a counter that must increase. They cover enroll, sign-in, a stale passkey
being refused as a clone, `launch_url`, a missing passkey control, delivery to a data client and to a
file, and that nothing secret is logged. A page shaped like e-shiten's (user ID box, image-only
control, ceremony in a named popup that reports to its opener) covers typing the user ID, a missing
box, and a second ceremony in a popup after a first one. They skip when Chrome is absent. They were run on macOS, on native Windows 11 (Windows' own Node,
Python and Chrome; the full suite including the real Credential Manager) and, in CI, on Linux.

What is not verified against any real broker: each site's login URL (SBI's redirected to a
maintenance page when checked, SMBC Nikko's and Monex's refuse a bare request), the passkey control's
text, how BRiSK is launched from the main site, the name of the BRiSK session cookie (only SBI's is
known, so the other sites accept any cookie on their BRiSK host once it appears), whether a broker
accepts a virtual authenticator at registration or asks for more identity checks, whether it refuses
an automated Chrome (see above), and how long the cookie lasts. Each is an overridable default or a
manual step, and a wrong guess ends in an explicit error. e-shiten's login page is the one whose markup was
read (below); the rest of that list applies to it too. The automated runs on Windows are headless; the
visible Chrome window of `enroll` and `login` was also tried by hand on Windows 11 (the real console
prompt, Credential Manager) and on macOS. The DevTools `WebAuthn` domain is marked experimental.

The mechanism itself is verified against a real site that is not a broker, webauthn.io (a public passkey
demo; it logs you in on load when a passkey is present, which `login` catches because it listens before
loading the page): registration through the virtual authenticator, `brisk login` with its cookie and the
advancing sign counter, on macOS and by hand on Windows. A `sites.json` entry for it needs only
`login_url` `https://webauthn.io/`, `cookie_host` `webauthn.io`, `passkey_button` `Authenticate` and
`cookie_prefix` `sessionid`.

### Tachibana e-shiten's sign-in

Mapped on 2026-10-11 from e-shiten's public pages, without an account. Its PC login
(`www.e-shiten.jp/passkeylogin.html`) offers 「パスキーでログイン」, a link to
`tr2.e-shiten.jp/e-shiten?auth=passkey`, and 「電話番号認証でログイン」, a link to the same address without
the query. The passkey form (Shift_JIS, jQuery) asks for the user ID (`kokyakuUserId`, letters and
digits) before the passkey. Its control, 「パスキーを使う」, is an `<a href="#">` around an `<img alt>`,
with no text of its own. Its handler checks the ID, opens `window.open('about:blank', 'passkey')`
and submits the form into it. The ceremony runs in that popup, which `postMessage`s the result
(cookies, SID) to the opener, and the opener submits it to finish signing in. So the
site entry is `login_url` the passkey form, `user_field` `input[name="kokyakuUserId"]` and
`passkey_button` 「パスキーを使う」. Enter in the ID box also presses it.

Registering needs a sign-in first: user ID, PIN and phone-number authentication at
`tr2.e-shiten.jp/e-shiten` (the `enroll_url`). Tachibana says the registration screen follows that
sign-in, and that passkeys are managed under お客様情報 › 設定情報 › パスキー認証. It also says
passkeys are required for trading and withdrawals from 2026-12-12, and that a withdrawal asks for
the passkey again (in the same kind of popup).

Not verified: whether e-shiten accepts a virtual authenticator at registration (it says Microsoft
Password Manager cannot register one, which may mean it filters authenticators); how BRiSK Next is
opened from the signed-in site (there is no `launch_url`, so it is opened by hand in the window); and
the BRiSK Next session cookie's name. `cookie_prefix` is `session_` because the two sites serve the
same build, and SBI's cookie is `session_…`. If it is wrong, `login` ends with "No
next.e-shiten.brisk.jp session cookie appeared", and `cookie_prefix: ""` in `sites.json` accepts any
cookie of that host. Because e-shiten allows one BRiSK session, the one opened during sign-in is the one
the feed then uses: Chrome is closed by then.

## Reconstructing a recording

To validate and reconstruct a saved or downloaded recording with the Rust state
(`brisk_recording` from a release archive, or built from source), export its batches
first:

```sh
brisk export recordings/downloaded --jsonl /tmp/events.jsonl
rust/brisk_quote_ingest/target/release/brisk_recording \
  --input /tmp/events.jsonl --latest /tmp/restored-state.json
```

This rebuilds final state for offline analysis. It does not retimestamp data or
simulate original exchange delivery.

## Shared archive

The public configuration is [briskapi/archive.json](briskapi/archive.json): bucket
`brisk-recordings-honvl-tokyo` in Tokyo (`ap-northeast-1`). Anyone can read
published `archive/` and `timing/` objects and list those prefixes over HTTPS. `incoming/`
(staging) is private. Deployment is described in [infra/README.md](infra/README.md).

### Integrity

The client is open source, so the service validates every upload. Demo and
synthetic content must match a reference; SBI live content has no reference and
is contributor-declared:

- **Reference replay.** Market content must equal the pinned demo replay.
  [briskapi/references/historical_mock.json](briskapi/references/historical_mock.json) holds a
  truncated SHA-256 chain per security (master entry plus every update and the
  batch it arrived in) and a hash of the batch clock timeline; it contains no
  market data. Any subset of securities is accepted, but only complete replays.
  `tools/brisk_mock/build_reference.py` regenerates it after a re-audited asset pin.
  This comparison applies only to demo/synthetic sources, not to `sbi_live` or `eshiten_live`.
- **Canonical tables.** Each table must have exactly its schema and metadata, a
  pyarrow writer, zstd, full row groups and no bytes between its column chunks.
  Every row group must equal the encoding of the validated content, including the
  columns reading ignores (`ts_init`, `flags`, v2's constant Equity fields), null for
  every zero price and rows sorted by issue, then batch. Packaging converts recorder
  output to this form and rounds local timings to microseconds.
- **Exact fields.** Batches, master entries and quotes must have exactly the
  fields defined in `briskapi/schema.py`. Account, session or unknown fields are
  rejected. Local timing values must be plausible: decode ≤ 10 s, receipt clock
  monotonic within 24 h, millisecond values with at most microsecond precision,
  and pacing consistent throughout. End-of-stream counts must match.
  SBI quotes use null
  replay lateness. Its end frame count includes pre-bootstrap catch-up frames,
  so it can exceed the number of emitted quote batches. SBI packaging removes
  connection/decoder diagnostics and publishes only
  `{"kind":"sbi_websocket","origin":"https://sbi.brisk.jp/"}` as transport metadata.
  The service rejects any additional transport fields.
- **Verified tables, the service's zip.** The service publishes the uploaded tables
  once verified, inside its own deterministic zip, so zip comments, extra fields and
  timestamps never reach the archive. The tables themselves are not rewritten: what
  table verification can't see (Parquet statistics, unused dictionary entries,
  encoder choices in pages) is the one place an upload could keep a few bytes of
  its own.
  `synthetic_test` accepts only the fixed one-security probe in `briskapi/schema.py`.
- **Nothing lingers in staging.** Each upload is deleted right after validation,
  whether accepted or rejected. A ticket admits only its first object version;
  later POSTs with the same upload form are deleted on arrival.
- **Malformed input.** Corrupt zips, unexpected zip members, compressed members and
  unreadable Parquet are rejected like any other invalid upload. Row counts, list
  lengths and declared decoded sizes are checked before data is read.

A client chooses its alias (up to 64 characters), its license and bounded timing
measurements. These cannot be verified. SBI master names and market values also
remain unverified contributor content; exact fields and canonical encoding do
not establish market accuracy or redistribution permission. Upload quotas apply
to every source.

### Publication and limits

- Upload tickets last 15 minutes and bind the object key, exact size and encryption.
- Uploads are bound to their first S3 object version. Retries of that event are
  safe, and overwriting an upload cannot replace a published dataset.
- Recordings are content addressed: `archive/YYYYMMDD/SHA256/`, where SHA-256 is
  that of the published `recording.zip`. Publication writes the data first and the
  immutable manifest last, as the catalog commit marker.
- Limits: a 64 MiB package, 1 GiB in canonical JSON form and 16 MiB per batch in
  that form. Tickets are limited to four per IP address per hour (stored as a keyed
  hash, never the address), 64 per hour in total and 5 GiB of authorized uploads per
  UTC day. A complete demo replay is a 34 MB package.
- Ticket metadata in staging expires after two days. Published data is retained.
- `pull` checks the package's size and SHA-256, the tables, the manifest and the
  reference replay where available, then moves the folder (the tables and the
  manifest; the zip isn't kept) into place atomically.
  Manifest validation and archive identity checks precede payload download.
  Listings warn and skip invalid/unsupported manifests, including old probes
  with incomplete reference metadata, without hiding S3/network failures.

### Sources

The decoded schema accepts `historical_mock`, `synthetic_test`, `sbi_live` and
`eshiten_live`. The small, self-authored `synthetic_test` fixture verifies publication;
filter by source when selecting market data. Opted-in decoded broker recordings retain
`source=sbi_live` or `eshiten_live` and undergo canonical-field, integrity and continuity checks.
Their market accuracy and redistribution permission are contributor-declared;
they are not silently treated as reference-verified demo data. New raw-wire
submissions remain unsupported. The existing legacy wire capture is labeled
`sample_data`, with schema `brisk-sbi-wire-jsonl-v1`, for protocol research. It is
separate from the validated decoded-recording catalog and retains its own manifest.

## Development and deployment

Tests and conventions are in [CONTRIBUTING.md](CONTRIBUTING.md). Archive
deployment, IAM scope, costs and removal are in [infra/README.md](infra/README.md).
Archive changes reach contributors only after `infra/deploy.py` runs.
