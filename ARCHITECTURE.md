# Architecture

How briskapi works, what the data means and how the private archive protects
itself. For usage, see the [README](README.md).

## Components

| Component | Where | Role |
| --- | --- | --- |
| Decoder hosts | `briskapi/decoder/` | Run BRiSK's own WASM decoder in Node, without a browser or Chrome DevTools. `decoder.cjs` replays the public demo, whose assets must match SHA-256 pins in `assets.json`. `brisk.cjs` drives a broker's BRiSK decoder (SBI's, e-shiten BRiSK Next's, Matsui's, Monex's or SMBC Nikko's) from an authenticated session, one trading day after another, reading a large selection's quotes with worker threads (`workers.cjs`). Both emit one JSON batch per frame. |
| Python API | `briskapi/` | `Recording` (files), `Feed` (live, background thread, with a worker thread that packages and uploads each finished day), `Ticker`/`Market` views and `Archive`. |
| BRiSK site client | `briskapi/brisk.py` | Cookie login (SBI by default, or another broker with `site=`: `e-shiten`, `matsui`, `monex`, `smbcnikko`), token boot and the REST data behind `Ticker.candles`/`margin` and `Market.turnover`/`lists`/`events`/`schedule`/`watchlist`; `briskapi.connect(site=...)` starts the live host through it (`_connect`). |
| Archive client | `briskapi/cli.py`, `briskapi/schema.py`, `briskapi/_parquet.py`, `briskapi/identity.py` | The `brisk` CLI, upload consent, your identity, Parquet packaging, upload, verified download, deletion, restore and export. |
| Auction state | `rust/brisk_quote_ingest` | Optional Rust tools: `brisk_quote_ingest` validates batches, keeps per-security state and publishes latest-state files; `brisk_recording` reconstructs a saved recording. |
| Archive service | `archive_service.py`, `infra/deploy.py` | A Lambda function URL serves each owner's uploads, listings, downloads, deletions and restores; S3-triggered ingest validates and stores and keeps each owner's OHLCV bars in step; DynamoDB holds quotas. |
| Cloud API | `query_engine.py`, `query_service.py`, `briskapi/_historical.py` | A second Lambda function URL answers Databento-style queries over an owner's recordings and the demo's, with OHLCV bars built from every broker upload; `briskapi.Historical` and `brisk query` are its client. The engine and the service run only in the cloud and are not in the wheel. |
| Records | `briskapi/records.py` | The Databento-style schemas (`FIELDS`) and the projections from a recording's tables to them, shared by the cloud API's engine and `briskapi.Live`, so a record is the same in history and live. |
| Live gateway | `briskapi/_gateway.py` | A feed's server on 127.0.0.1 that hands its batches to `briskapi.Live` sessions on the same computer; `brisk live` runs one by default. |

```
demo assets (HTTPS, pinned) ─► decoder.cjs ─┐
broker BRiSK session ────────► brisk.cjs ───┴─► JSON batches ─┬─► briskapi.Feed: live state, callbacks, iterators
                                                             │     └─► gateway (127.0.0.1) ─► briskapi.Live: Databento-style records
                                                             ├─► brisk live: JSON lines
                                                             └─► briskapi.record ─► Parquet recording (demo only)

JSON batches ─► Parquet tables ─► zip ─► upload ticket ─► S3 staging ─► verification ─► archive/OWNER/… ─► briskapi.pull
                                                                                         (a replay's market tables: reference/)

archive/OWNER/… + reference/ + bars/OWNER/… (built from the owner's live recordings) ─► query_service ─► briskapi.Historical, brisk query
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
2. `history` (only after a snapshot that restores trades, as a live session's started
   mid-day can): those trades, up to 50,000 per batch, right after the bootstrap and at
   its clock. The bootstrap's volumes already count them.
3. `quotes`: one batch per frame with the quotes that changed in it and its trades.
   Empty batches are kept, so sequence continuity and feed health stay separate from a
   quiet security's last update.
4. `end`: frame and update counts, written only after a clean finish.

That is one trading day. A demo replay is one such stream; a broker's live host emits
one per day, back to back, each numbered from its own bootstrap (see
[Trading days](#trading-days)).

Sequence gaps, clock regressions, malformed input and interruption invalidate a
stream in the Rust state and in archive validation. The Python feed checks
sequence continuity within each day and a clean end of each.

Recording time queries filter each quote by its own `source_time_us`, including
quotes in batches whose clock is later than the query. Batch clocks are upper
bounds on contained quote times, so stopping at the first later batch would miss
valid snapshots and deltas. `Market.summary()` uses the first and last batch
clocks, including empty batches and the end marker; recordings cache that range
when no manifest is available, while feeds track it under the state lock.

## Recording format

For users, [docs/data-format.md](docs/data-format.md) describes every column of recordings and
of cloud API records; this section is the internals.

A recording is a directory of four Parquet tables (`briskapi/_parquet.py`), laid out
like a NautilusTrader v2 catalog's `data/`. Each table is v2's own schema with BRiSK's
fields added after it. Rows carry v2's `identifier` column (`CODE.XTKS`), as v2
returns multi-instrument queries, instead of one directory per instrument: a
directory per security would make the complete demo 8,262 small files and about three
times the size. Reading gives back the decoder's batches exactly, so validation and
reference fingerprints are the same for every format.

| File | Rows | Columns |
| --- | --- | --- |
| `data/order_book_depths.parquet` | One per quote update, sorted by issue, then batch | v2 `OrderBookDepth`: `bids`, `asks` (up to ten levels each, best first: `price`, `size`, `count`, `order_id` 0), `flags` (16, plus 32 on the bootstrap snapshot), `sequence` (the frame number), `ts_event` (source time), `ts_init` (the same). Then `identifier`, `issue_id`, `seq` (the batch), `max_frame`, the prices (`last_price`, `open_price`, `bid_price`, `ask_price`, `indicative_price`, `closing_indicative_price`, `indicative_open_price`, `reference_price`; null for the vendor's zero), the counts and enums (`indicative_volume`, `indicative_imbalance_side`, `closing_indicative_volume`, `closing_indicative_imbalance_side`, `quote_flag`, `quote_side`, `volume`, `issue_status`, market and closing market buy/sell quantities, `bid_quantity`, `bid_orders`, `ask_quantity`, `ask_orders`, and the board totals `bid_total_quantity`/`_orders`, `ask_total_…` and their `bid_closing_total_…`/`ask_closing_total_…`), `special_quote_time`, `turnover`, and the closing-condition orders' books `closing_bids` and `closing_asks` (shaped like `bids` and `asks`) |
| `data/trades.parquet` | One per execution, sorted by issue, then in the decoder's order | v2 `TradeTick`: `price`, `size` (shares), `aggressor_side` (BUY for the decoder's type 1, SELL for 2, NO_AGGRESSOR otherwise), `trade_id` (the issue's frame number and the trade's place in it), `ts_event`/`ts_init` (the trade's time). Then `identifier`, `issue_id`, `seq` (the batch), `frame` and `side` (the raw type) |
| `data/equity.parquet` | One per security, by issue | v2 `Equity` (`id`, `raw_symbol`, `currency` JPY, `price_precision` 1, `price_increment` 0.1, `lot_size`, `max_price`/`min_price` from the daily limits, `ts_event`/`ts_init` at JST midnight; `info`, JSON in v2, is a null string), then `issue_id`, `tick_type`, `issue_type`, `name`, `base_price` |
| `data/custom/BriskBatch.parquet` | One per batch | `seq`, `kind` (bootstrap, history, quotes or end), `received_unix_ms`, `decode_ns`, `replay_lateness_ms`, `skipped_issues`, `frames`, `quote_updates`, `replay_wall_ms`; on the bootstrap row `source`, `trading_date`, `input_transport` (canonical JSON), `source_timestamp_origin`, `exchange_delay_ms`, `market_issue_count`; v2 custom data's `type`, `ts_event` and `ts_init` |

Prices and level sizes are `decimal128(38, 16)` as in v2; times are UTC
`timestamp[ns]`. `ts_init` repeats the source time rather than a local clock, since a
demo replay's receipt clock is years after its market time; receipt times stay exact
in `BriskBatch.received_unix_ms`. Tables are zstd-compressed in row groups of 65,536
rows, so one security's history reads only its row groups. The complete demo replay
(735,382 updates and 92,263 trades) is 41 MB, against 965 MB of JSONL.

`brisk export DIR --nautilus CATALOG` writes v2's layout: `equity`, `order_book_depths`
and `trades` per instrument, readable by v2's `ParquetDataCatalog`, and the rest of each
quote as custom data `BriskQuote` (int64 columns in vendor units, so a v2
`customdataclass` can declare them, and the closing-condition books as JSON text).
`--jsonl FILE` writes canonical batches.

## Live feed

`briskapi.Feed` reads decoder output on a background thread and applies each batch
to its state under a lock. A new listener takes its snapshot and registers in one
locked step, so it can't miss or duplicate an update. Iterators use a bounded
queue (10,000 items) and callbacks run on the feed thread, so a slow consumer
slows the decoder instead of dropping updates. An abandoned iterator never blocks
the feed. An exception in a callback stops the feed and is re-raised by `wait()`.
A feed keeps current state only, unless `history=True` retains every update per
security.

A feed records one trading day at a time. It splits the stream into days, each a
`bootstrap ... end` numbered from its own bootstrap: the sequence check admits a new
bootstrap only after an end, and at it the feed's state resets (the quotes, and with
`history=True` the retained updates and trades) and its timing statistics start again.
A demo replay is one day; a broker's live host (see [Trading days](#trading-days)) goes
on to the next.

When uploads are enabled, `output` is given or the feed has a gateway, the feed also writes
each day to a spool as it arrives (`briskapi/_spool.py`; a gateway replays from it, and a day
kept only for that is not packaged). The spool is the recording's own Parquet tables, a part
at a time, under `~/.local/share/brisk/spool/segment-ID/`: the equity table with the
bootstrap, then `parts/NNNNNN/` with the depths, trades and batch tables of the batches since
the last part. A part closes every `PART_SECONDS` (30), or after `PART_BYTES` (64 MiB) of decoder
output, and at the segment's end. Its lines are parsed, made canonical as `package()` makes them
(so no connection diagnostic reaches the disk) and turned into columns from the moment it closes
(`part_columns`; without the GIL in slices, on up to `_parquet._THREADS` threads: a third of the
cores, at most four, as the feed and its host come first); a writer thread of its own then writes the
parts in order (`write_part`, which numbers trades on from the part before), fsyncs each and renames
it into place, so a crash loses at most the part still open. A day's
parts take about the disk of its recording; its decoded JSON, which the feed used to keep in a
temporary file, takes some twenty times more. At the day's end the feed hands the spool to a
worker thread (a `ThreadPoolExecutor` of one, so days finish in order) and reads on. The worker packages
the day, keeps it in `output` as `SOURCE_YYYYMMDD_FROM-TO` (`schema.local_name`; FROM and
TO are the day's first and last source clock, JST HHMMSS; a folder already there is left
alone, with a warning), uploads it to the owner's private archive and uploads its
timing summary, as chosen, then appends its result (`trading_date`, `name`, `path`,
`uploaded`, `timing_uploaded`) to `feed.recordings`, calls `on_day(result)` and deletes
the spool. Packaging reads the parts back (`load_parts`: each part's tables decoded, joined
and put in the canonical order) and writes the same bytes as the day's batches at once would;
`tests/test_spool.py` checks it. Its manifest's summary comes from those columns
(`Columns.summary`) rather than from the archive's checks run here, which took the demo's three
minutes 35 seconds and would take a day most of an hour: the archive runs them on every upload, and
an upload it rejects is kept like a failed one.

A broker's day is shipped as it runs, in pieces (`Spool.ship`): a whole-market day has some 50
million quote updates, against a package of at most 64 MiB that the archive's function checks in
memory (the demo's 735,000 make 39 MiB and take 1.7 GB of its 3 GB), and packaging it at once would take
a computer's memory and most of an hour. After each part is written the spool tells the feed's worker,
which ships a piece once the parts not yet shipped hold `SEGMENT_ROWS` (500,000) quote updates and end
where every issue's trades so far are followed by a quote of it (`_parquet.part_index` reads that from
the parts' small columns): the archive checks that the trades account for each quote's volume and
turnover, which a piece ending between a trade and its issue's next quote would break. Each piece is a
recording of its own (`_parquet.after`): a bootstrap like the day's, at the clock of the batch before
it, holding each issue's last quote so far (`_parquet.latest`; an issue with none is skipped), then the
piece's batches numbered on from it and an end (`repair_end`'s, or the day's own with its own count,
`recount_end`). Consecutive pieces cover the day without a gap (each starts at the clock where the one
before ends), and the cloud API reads them as one: its bars merge their clock ranges, and the quotes a
bootstrap repeats carry the frame numbers already seen, so its deduplication drops them. The worker
keeps and uploads each piece as it would a day, and at the day's end ships what is left (in more pieces
if a stop or a slow worker left more than one's worth). `shipped.json` counts the parts shipped, so a
crashed session's spool is finished from there; where the shipped pieces left the book is rebuilt from
their parts a part at a time. A demo's replay is one recording, as its reference requires.

A packaging or upload failure is a warning and a result that
says so, never a failed feed, and the day is then kept in `~/.local/share/brisk/recordings`
(its package, for `brisk upload` to retry, or its tables); neither is an `on_day` callback that
raises. `brisk live --output
DIR` prints each result as `{"day": ...}` on stderr. Broker feeds do the
same with their market recording and also accumulate timing statistics
(`briskapi/timing.py`). Uploads are on by default
(`cli.consented`): with no saved choice the CLI asks once, on a terminal, before the first
session (Enter accepts), and a run that can't ask (the Python API, a CLI without a
terminal) uploads by default after a once-per-process notice, without saving a choice. `upload_market_data=False`
(`--no-upload-market-data`) keeps a broker session's market recording local;
`BRISK_UPLOAD=0` or `upload=False` (`--no-upload`) disables both uploads. Only a segment that
reached its end, a day or the part of one before a lost connection or a stop (see
[Trading days](#trading-days)), is packaged and uploaded. `close()` stops a broker feed by asking
its host to end the running day (SIGTERM, which `brisk live` also gets from Ctrl-C, `kill` or a
shutdown) and waits up to 30 s for the host to exit, its end written and read, before killing it;
a demo replay is killed at once (a partial replay is never uploaded). A broker segment that is
still unfinished when the feed closes or fails (a host that crashed, was killed or did not end in
time) is repaired: `_parquet.repair_end` appends the `end` its host never wrote, and the feed
finishes it like a day (`_finish_segment(repair=True)`), its result marked `repaired`. The repaired
end's `quote_updates` counts the segment's quote updates exactly, as the host does; the host's
frame count and wall time died with it, so `frames` is the least the archive's check allows (one per
batch after the bootstrap) and `replay_wall_ms` the span of the batches' `received_unix_ms`. A demo's
segment is dropped, being the same for everyone. Each spool records its session's choices
(`intent`: upload, output) in `owner.json`; a spool a crash left behind (its owner process gone) is
finished the same way by the next feed to start (`_spool.leftovers`, `Feed._recover`): uploaded if
its session uploaded and uploads are still on, kept in its session's `output`, and dropped if that
session kept nothing (a gateway's spool only). A segment that fails even so is kept in
`~/.local/share/brisk/recordings`. `wait()` and `close()` return after the days already finished
are kept and uploaded.

## Live gateway

A feed started with `gateway=True` (`brisk live` unless `--no-gateway`) serves its batches
to `briskapi.Live` sessions in other processes on the same computer. Only the feed signs in
to the broker; a session needs the gateway's key and nothing else.

- **Reach.** `Gateway` listens on 127.0.0.1 (an ephemeral port) and writes its port and a
  random key (`secrets.token_urlsafe(32)`) to `~/.config/brisk/gateway/DATASET.json`, created
  owner-only (0600) and replaced atomically. `Live` reads that file. A session's first line
  must carry the key (compared in constant time), the protocol version (1) and the gateway's
  dataset, or it is answered with an error and closed. A file whose gateway no longer answers
  (a killed feed) is replaced by the next one; while one answers, a second feed for the same
  dataset serves nothing, with a warning. Closing the feed removes the file. A broker feed is
  `XTKS.BRISK`, the demo `XTKS.BRISKDEMO`, as in the cloud API.
- **Protocol.** Newline-delimited JSON. After `{"gateway": "session"}` acknowledges the key, the
  session may send subscriptions at any time: `{"subscribe": [ids], "symbols", "replay",
  "snapshot"}` (`start()` sends those made before it as one). Each is answered in the stream:
  `{"gateway": "subscribed", "id"}`, with the day's bootstrap (master, without quotes and
  trades) if the session lacks it and, if asked, those symbols' latest quotes; then, if asked
  and the day is running, the day so far and `{"gateway": "live", "id"}`. Every batch follows as
  it arrives, and last `{"gateway": "closed", "status", "error"}` when the feed ends. Batches
  are the decoder's own lines: unchanged once a session asked for all symbols, otherwise
  rewritten with only the quotes and trades of the symbols subscribed so far (the master
  whole; a batch left empty is skipped).
- **No batch missed or repeated.** `Feed._run` adds each line to the day's spool, applies it,
  then calls `Gateway.publish`, which updates the gateway's own bootstrap, latest quotes and the
  count of lines published, and queues the batch for every session, all under one lock. A
  subscription widens the session's symbols, takes the bootstrap, the latest quotes and a
  replay of the spool's first that-many lines under that lock (`Spool.replay`: the closed parts,
  read back and re-encoded, then the lines not yet written, as the decoder wrote them), and
  queues its answer (written out, with the replay, on the session's thread). So its answer
  comes after every batch the session got without its symbols and before every batch with
  them, and a replay meets the live batches exactly; batches that arrive meanwhile wait behind
  it. At a day's end the gateway stops offering replays: the worker packages and deletes the
  spool, and a replay already under way keeps it until it is read.
- **The fast lane.** A broker feed's recording reads every quote of the whole market, frame by
  frame, and at the opening auction it can fall behind the stream. The host (with
  `BRISK_LANES=1`, which the feed sets when it serves a gateway) also runs a pool of its own
  for the fast lane, a quarter of its quote workers (at least one). Its decoders are fed every
  frame, read the quotes of the symbols Live sessions are on the lane for, as each frame
  arrives, and write them at once as out-of-band lines (`"lane": "fast"`) of the frame's
  batch number, ahead of the recording's batch. Lines come in one stream, so the feed reads
  them in order with the recording's; the feed passes them to `Gateway.lane`, never to the
  recording. The gateway tells the host on its stdin which symbols the lane reads
  (`{"fast": [codes]}`). A symbol added while the day runs is read once at once
  (`"type": "join"`, its quote after the lane's last frame), and the subscription waits for that
  answer, so its snapshot and its updates meet. Before each session's bootstrap the host says
  whether it has the lane, for how many symbols (500 per worker) and which it reads
  (`"type": "lanes"`).
  A Live session is on the lane from its first subscription if that asks for no replay and, with
  the lane's other sessions, for no more symbols than it takes; `ALL_SYMBOLS` never is. Until
  the host has said, a session that would fit gets the recording's batches; at that word
  (before any batch of the session) it moves. A session on the lane gets the recording's
  bootstrap, history and end, but its quotes from the lane only. One whose later subscription
  does not fit leaves it for the recording's batches: up to the last batch the lane brought it,
  those batches without the symbols it had there (`fast_until`, `old_codes`), and its new
  symbols' snapshot from the recording's state, so it misses nothing and gets nothing twice.
  A symbol's updates come from one lane at a time, in frame order, so trade IDs (an issue's
  frame and the trade's place in it) and bars are as the cloud's. Tested from the host
  (each fast line is the recording batch's quotes and trades of those symbols, a join the
  recording's state) through the gateway (records equal to the recording's, across a join and
  a move back) to a live host behind the feed.
- **Slow sessions.** Each session has its own sending thread and queue. The feed never waits
  for one: a session more than `MAX_BEHIND` (1 GiB) behind has its queue dropped, is sent an
  error and is disconnected (after 10 seconds if it still reads nothing), so a stalled program
  cannot slow the feed or its recording. At most 32 sessions at once.
- **Records on the client.** `Live` projects each batch itself, so the feed process does no
  work per session beyond queueing (and, for named symbols, rewriting) lines. It builds a
  batch's depths and trades tables with `_parquet.stream_tables` (the recording's own row
  encoding, in arrival order) and the day's equity table once, then applies
  `briskapi.records`' projections: the same code the cloud API runs over stored tables, which
  is why a live record equals the stored one. `ts_recv` is the batch's `received_unix_ms` for
  `XTKS.BRISK` and `ts_event` for the demo, as in the cloud. Trade IDs (`FRAME-PLACE`) carry
  each issue's last frame and place across batches (`Columns.trade_ids(carry)`), so a replay
  from the day's start numbers them as the stored recording does. A session that named
  symbols skips, unparsed, replayed lines that mention none of its codes. Records come per
  batch, sorted within it as the cloud API sorts (`ts_event`, `instrument_id`, `sequence`);
  across batches, in arrival order. `start` filters on `ts_event`; definitions (stamped at
  JST midnight) are given when a subscription starts, or by a replayed bootstrap, and at each
  new day. A snapshot is projected as a bootstrap batch (`seq` 0), so `mbp-10` flags it 32. A
  replay goes to its own subscriptions only, projected against a day of its own (fresh trade
  IDs), while the others wait for the live batches behind it.
- **Bars.** Each bar subscription aggregates its trades per (code, bar start) as the cloud's
  `build_bars` does: the open is the first trade by (`ts_event`, frame, place in the frame), the
  close the last, with high, low and summed size. A bar goes out once BRiSK's clock reaches its
  end, with no allowance for trades stamped earlier than the clock: a trade arriving after its
  own bar has ended (by the clock the bars were last given to) goes into the next bar still
  open, so its volume counts and no bar for a past interval goes out late. The cloud places such
  a trade by its own time, so the two can differ there. That clock is the
  latest batch's, or, between batches, this computer's less the feed's delay: `_Latency` keeps,
  for each live batch, when the session took it in less BRiSK's stamp (network, the gateway's
  hop and any clock difference) and allows for the largest of the last 10 seconds, and
  `Live._tick` wakes when the next bar is due. Without that, a session that named a few
  symbols, which receives only their batches, would hold a quiet symbol's bar until its next
  update. The tick pauses during a replay, whose live batches are still queued. Bars obey the
  cloud's coverage rule (`records.session_pieces`, shared): a bar is given only if its whole
  span within the trading sessions lies between where the session's trades begin (the day's
  bootstrap when it saw the day from the start, by replay or a new day; else its first batch)
  and the clock, so a bar is never partial; a replay of the demo, whose cloud bars have no such
  rule, gives every bar. The day's `end` batch gives what remains.
- **Delivery.** Callbacks and streams run on the session's thread; a callback's exception goes
  to its `exception_callback` or a warning. Iteration uses a bounded queue (10,000 records)
  that blocks the session's reader, and through TCP the gateway's queue, never the feed.
  `stop()` shuts the socket down. A session that loses its connection without the closing line
  ends with an error, as does one whose feed failed.

## Timing reports

A timing report is uploaded even when a broker session keeps its market
recording local. A report (`brisk-timing-v2`) holds p50/p90/p99/max of decode time, feed-clock age
at receipt and frame spacing, a stall count (gaps over one second), the frame count,
trading date, first/last JST minute and client version. It covers one day: a session
that records several uploads one report per day, each when its day ends, and a day cut
short by closing the feed gets one for the part recorded.

The API's `timing` action takes the report. The service checks exact fields, bounds and
microsecond precision, monotonic percentiles, at least 100 frames and a trading date within
the last 30 days; rate limits it (six per IP address per hour, 600 in total); and writes its
own canonical JSON to `timing/OWNER/YYYYMMDD/SHA256.json`, so identical reports deduplicate.
A report is at most 2 KB. `briskapi.Archive().timing()` reads the owner's back.

## Broker BRiSK

The endpoint sequence was learned from pybrisk: session cookies authenticate
`/api/frontend/boot`, which returns a bearer token; `/api/app/boot` then gives
the trading date, series, schedule, WebSocket URL and master/snapshot hashes.
Requests are rate limited and never follow redirects, because a redirect means
an expired session and following it would forward credentials. `briskapi.connect(site=...)`
asks `/api/frontend/boot` before it starts the live host, so an expired session
is one `SessionExpiredError` line and Node never starts. Cookies given as JSON
(the `BRISK_*_COOKIES` variables or a saved cookie file) that don't parse are
the same error, naming the variable or file but never the value.

The live host (`briskapi/decoder/brisk.cjs`) uses the same session to fetch the
master and snapshot, finds the site's decoder through the app's own bundles (it is
served only to logged-in sessions and changes with the broker's releases, so it can't be
pinned), checks that it exports every function the host calls, and decodes
WebSocket frames with protocol version 18000. Cookies reach it through the
environment (the site's own variable, such as `BRISK_SBI_COOKIES` or `BRISK_ESHITEN_COOKIES`;
`BRISK_SITE` names the site), never the command line. One table names the brokers in each place,
`SITES` in `brisk.py` and in `brisk.cjs`, with their sources and transports in `schema.py`
(`tests/test_brisk.py` checks they agree): address, label, cookie variable, `source` and
transport `kind`.

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
bundle) and in pybrisk's SBI research (`engineio.cjs` and `brisk.cjs`):

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
   first one is a connection failure, as in the vendor client. Any frame arriving also
   counts: the vendor client decodes each frame on arrival, but the host decodes in
   order with its output, and in the opening burst frames (heartbeats among them) can
   wait behind the output for longer than 10 s, which once ended the session at
   09:00:07 although the link was alive. A second rule is the
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
16000 (the demo) is the vendor client's own layout. For 18000 (the brokers' BRiSK) the stock
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
at its own bid and ask.

`_getItaRows` is a rendering helper for the board on screen, and costly: it looks up the issue's
board object, rebuilds it if a frame changed it (0.3-0.5 µs), then walks every tick of the
ladder for OVER and UNDER, converting each to a price through the issue's tick bands and adding
notional columns (quantity × price) on the way. A call costs the same for 1 row or 400, about
10 µs on average and 28 µs on Toyota's 3,521 ticks. So the decoder reads books itself
(`BoardReader`) and uses `_getItaRows` only to check it, or in its place when it cannot be used:

- **Where the ladder lies.** Each board holds its ladder in a `std::deque` of 72-byte levels, 56
  to a block, one per tick from a first tick index the board keeps. A level holds the quantities
  (f64) of its bids and closing-condition bids at +8 and +16, their order counts (u32) at +24 and
  +28, and the same for the asks at +40, +48, +56 and +60; the market orders are a level of their
  own in the board, just before the deque. The builds lay the board out differently (the demo's
  deque at +92, SBI's of March 2026 at +100), so `boardCalls` reads `_getItaRows`' code for what it
  calls: the board lookup (a slot of the function table and a registry address, called through
  `dynCall_iiii`), the flag that marks a board rebuilt, and the rebuild (`dynCall_vi`). Then
  `locate()` finds the deque's block map, first slot, length and first tick index in the board
  object, and its market level, by matching issues' levels to `_getItaRows`' rows.
- **What it reads.** The board totals are summed by a kernel of ours, `board.wat` (SIMD,
  assembled into `decoder.cjs`), which shares the decoder's memory: `create()` makes the memory the
  glue would (its import's size) and hands it over. Each side's ten levels are read from the best
  end of the ladder, and their prices from `_getTickPrice10ToIndex` (the lowest price at that
  index, remembered per tick type). A quantity `_getItaRows` could not convert is a trap here
  too.
- **Checks.** At start every issue's book must equal `_getItaRows`'; every 4,096th book after
  that is checked again; anything amiss, or a build whose code does not read as expected, and
  the decoder reads books through `_getItaRows` for the rest of its life (the host's
  `--trace-protocol` says which). With no orders on any board yet (before the pre-open), it is
  looked for again as books are read.

Both builds' books are the same both ways: the demo's whole replay (its reference digests), every
book through the demo's opening auction, and the 2026 build with the demo's snapshot against the
demo decoder through `_getItaRows`. The kernel also finds where each side's orders begin, so the levels are read
from the best price outward. A quote now costs about 8 µs (70 µs when the
ladder was walked 32 rows at a time).

Trades come from the decoder's QR records, which it keeps on the host side: for each
execution it calls the imported `_qr_emplace(stock, type, time, price10, lots, lot, frame)`
(`stock` is the issue's `_getStockPointer`), reads its last price back through
`_qr_lastprice10` and withdraws a record with `_qr_pop_back`. The exported `_getQR` is a
stub that returns 0 in both builds, and `_getTrace` only lists changed issues. On the
demo's data every issue's volume and turnover change by exactly its records' shares (lots
times lot) and price times shares, frame by frame. Type 1 records lie at or above the
prevailing ask (93%) and type 2 at or below the bid (89%); type 0 is the first trade of
1,508 issues around 09:00:00.09 and 22 later prints, so it looks like an auction
execution. The record's time is the same clock as the quotes', 0-30 ms (median 2.8 ms)
before the quote that shows it. The host takes the records after each frame; one
withdrawn after it was emitted ends the session (none is withdrawn on the demo). The decoder
reads back only an issue's count of records (`_qr_size`) and the last one's price, so an issue
keeps the prices of all its records but the records themselves only while one could still be
withdrawn from those not yet taken: after the open a snapshot restores the day's trades so far
(about 4.6 million on 2026-10-12), which kept whole took some 460 MB in every decoder (now 51
MB), and quote workers let the restored trades go. They are appended one at a time (spread
into one call, millions overflowed the stack). Records a
snapshot restores (a live session's could hold the whole day so far) come after the
bootstrap in `history` batches of at most 50,000, by issue; the snapshot's quotes already
count them, so they stay out of the volume check.
SBI's 2026 build has the same QR imports, called from the same two routines (one per
execution, one with the containers' set-up), but its trades are unverified: the offline
capture comes without a snapshot, and without one the build sets up no QR containers.

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
731,251 views of the demo's stream replayed through both builds. Broker quotes record it
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
What is not verified against a live session: any of the above on a broker's own stream, the
18000 master layout on today's build (found on SBI's of March 2026; each start checks it
against the master file), the stream's opening messages and whether it is plain binary
as recorded. Live, the master and the snapshot load with the schedule; the WebSocket
upgrade to `api.brisk.jp` was then answered with HTTP 502 (400 without the `session`
parameter, 404 on the site host, and no HTTP/2 extended CONNECT) with every header
variant tried, all without the `flex` subprotocol, which the host now asks for; whether
that is all the server wants is not known yet. The Socket.IO fallback's parameters live in a
profile (`BRISK_PROFILE`, or `briskapi.connect(site=..., profile=...)`) and are unverified. The
profile can also name another catch-up format (`catchUp`).
`--trace-protocol` prints the connection with every token redacted, so a first
attempt shows what the server answers. Anything wrong ends in an explicit error (a
stream that never initializes, a refused namespace, a rejected catch-up, an
implausible stock view or master) rather than guessed data. A connection lost once a day
has begun reconnects ([Trading days](#trading-days)); other failures end the recording.

### Quote workers

Reading quotes is most of a live host's work. On the demo the WASM decodes the whole
recording in about a second, while reading its 731,251 quotes takes about 20 s. At the
opening auction the whole market changes within seconds (a live open has sent some 130,000
quote updates a second), so for a selection of 500 issues or more (a recording is the whole
market) the host reads quotes with worker threads (`workers.cjs`). By default there are as
many as the computer has cores less four, from two to eight; `BRISK_WORKERS` sets the number,
and 0 turns them off. When the feed serves a gateway, a quarter of them (at least one) read the
fast lane's quotes instead ([Live gateway](#live-gateway)).

Each worker runs a decoder of its own, loaded from the same files and boot response. It is
fed every frame the host's decoder is, in the same order, with the catch-up and begin()
where the host has them, and it reads the bootstrap's quotes of its share as the host's
decoder did. It reads the quotes of its share of each frame's changed issues
(`issue_id % workers`), as JSON, and clears its own trace and trade records. The host's
decoder does everything else: the stream's callbacks (ping replies, heartbeats), its clock,
the changed issues and the trades. With workers the host decodes each frame as it arrives
and queues its batch, and writes the batches in order as their quotes come back. It builds
each line from the workers' JSON, so the batches are byte for byte what one thread writes
(tested on the demo's whole replay, catch-up included). A worker must have traced exactly
its share of the host's changed issues, or the session stops: its decoder would have
diverged. Quotes waiting to be written may not pass 1 GiB; the session stops rather than
fill the memory when the reader falls that far behind. A worker's decoder holds about 270 MB.

Parsing the host's lines is most of the Python feed's work (about 9 µs a quote update on one
thread). Without the GIL (free-threaded Python 3.14) one thread reads the pipe a chunk at a
time (up to 1 MB), `PARSERS` threads (up to four) parse the chunks, and the feed's thread
applies the batches in order; the spool builds its Parquet parts on threads of its own (above). With
the GIL, the lines are read and parsed one at a time, and the spool's thread takes turns with
the reader. Threads that share a `set` take its lock on every use, so code run on several threads
(`Columns.from_batches`) compares with copies of its own: with the shared key sets eight threads
built columns 1.3 times as fast as one, with copies 2.7 times. On an M5 Pro, the demo's whole replay, served as fast as it is taken, goes through
it all (the host and its workers, the feed recording to disk and serving the gateway) at about
61,000 quote updates a second without workers and 236,000 with 4 (more do not add); a live
open sends some 130,000. Slower cores take fewer: there the opening can fall a few seconds
behind, which the status line of `brisk live` shows in its data time.

### Trading days

A recording covers one trading day, and `record()` in `brisk.cjs` runs one `live()`
session after another (`brisk live` until it is stopped). A day ends, with an `end`
batch, when:

- the site's boot serves a new date, or says the session is `done` (`session_status`; the vendor
  app stops reconnecting then): the host asks `/api/app/boot` every `DAY_CHECK_MS` (ten
  minutes). A failed check is only traced, since the stream's own checks catch a dead session;
- the stream's clock goes back by more than an hour (`NEW_DAY_US`), as a new day's
  would. A smaller step back is not a new day, and a recording with one fails the
  archive's market-clock check;
- the server closes the stream, cleanly (code 1000) or, once the decoder has called
  `marketFinished`, in any way;
- the host is asked to stop: `main()` handles SIGINT and SIGTERM by ending the running day
  (`stop.end`, the day's `endDay`) and `record()` then returns instead of waiting for the next
  day; with no day running the host simply exits, and a repeated signal changes nothing.

Any other close, and any failure of the stream once it has started (stale data, missing
heartbeats, a dropped WebSocket), ends the segment the same way under `record()`: `live()`
emits the segment's `end` and throws `Interrupted`, and `record()` connects again after
`RETRY_MS` (5 s, 10 s, 30 s, 1 min, then every 2 min; the waits grow while connecting keeps
failing). The new session boots afresh and starts from a new snapshot, numbered from 0, so the
segment before the drop is a recording of its own and the Python feed packages and uploads it
like a day; only the gap is missing, and the cloud API merges the day's recordings. What another
connection cannot mend stops it with the error (`fatal`: an expired session, another WebSocket
session for the user, a host or site layout that changed, unknown codes), and so does any
failure before the first segment started.

The next day begins by asking the boot until the site serves a later date than the one
just recorded, with its session running, then boots afresh: the frontend and app boots, the
decoder, master and snapshot, and the catch-up. A start when no session is running waits the
same way. The vendor app opens the stream only while `session_status` is `running`; after the
close it is `done` and the boot has no `ws_url` (the host used to resolve the missing URL to
`/undefined` and fail). As the app reloads a minute after `next_date` (both it and the boot's
`time` in unix seconds, so the wait is on the server's clock), the boot is asked again then, or
in ten minutes if that is sooner; once it is due, every minute (`STREAM_CHECK_MS`). The
host says so once per wait, on stderr and to Python as an out-of-band line
`{"type": "waiting", "date", "session_status"}` (no `seq`): the feed keeps it nowhere (not in
the spool, a recording or the gateway), shows it as `feed.waiting`, and until the first
bootstrap has the status `waiting`, during which `ready()` and `connect()` wait past their
timeout. A stop while waiting (Ctrl-C, `feed.close()`) just exits: there is no day to end. Its batches are numbered from 0,
so each day is its own `bootstrap ... end`, and the Python feed keeps it as its own
recording. The wait is plain HTTP, so a session that expires meanwhile, or any other
failed request, ends the recording with that error instead of waiting on; how long a
broker's session cookie lasts is not known. The wait, the roll and reconnecting are tested
offline against fake servers (a server that closes after the first day, a site that serves
the next date at a later check, boots with no session running before a day and after one, a
connection that drops mid-session, boots that fail and a session that expires before
reconnecting). The clock rule has no test, and none of it has run
against a live session.

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

`site="e-shiten"` selects it (`BRISK_SITE=e-shiten` for the host; saved cookies in
`e-shiten-cookies.json`, `BRISK_ESHITEN_COOKIES`). Its batches say
`source=eshiten_live` and `kind=eshiten_websocket`. Its sessions are uploaded as
SBI's are: a timing summary and, after each day's clean end, the market recording, under the saved
choice and with the same opt-outs; packaging keeps only `next.e-shiten.brisk.jp` as its
transport provenance (`BROKER_TRANSPORT` in `schema.py`). `brisk login` can sign in to it with a passkey (site `e-shiten`; see
[Tachibana e-shiten's sign-in](#tachibana-e-shitens-sign-in)).

### Matsui, Monex and SMBC Nikko

Matsui (`matsui.brisk.jp`), Monex (`monex.brisk.jp`) and SMBC Nikko (`smbcnikko.brisk.jp`) run
the same vendor's BRiSK on their own hosts. Checked on 2026-10-14 without an account: each answers
`/` with 401 to a request without a session, as `sbi.brisk.jp` and `next.e-shiten.brisk.jp` do,
and, like e-shiten's, none serves SBI's current app bundle by name, so each serves its own build.
The host never assumes a build: as on e-shiten, it reads the signed-in page for its bundles, finds
the decoder through them and checks its exports (a build it does not recognise is an explicit
error, not doubtful records), and the board reader works out the build's ladder layout itself
(`BoardReader` above), falling back to `_getItaRows` when it cannot. None of the three has been
tried with an account.

`site="matsui"`, `"monex"` or `"smbcnikko"` selects one (`BRISK_SITE` for the host; saved cookies in
`<site>-cookies.json`, `BRISK_MATSUI_COOKIES`, `BRISK_MONEX_COOKIES`, `BRISK_SMBCNIKKO_COOKIES`). Their
batches say `source=<site>_live` and `kind=<site>_websocket`, uploaded as SBI's are, with the site's
own origin as transport provenance. Their passkey sites (`briskapi/sites.py`) are bound to these
clients, so `brisk login` hands the cookies to `briskapi.login()`. Their cookie names are not
known, so `cookie_prefix` is empty and any cookie on the BRiSK host counts as signed in.

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
6. **Delivery.** The five built-in brokers have a data client (`briskapi.brisk`, with the site's own
   `site=`), so their cookies go to `briskapi.login()` and are saved as that site's cookies; those of
   a site added in `sites.json` are saved to `~/.config/brisk/cookies/<site>.json` (owner-only) for the person's own use.
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

## Private archive

The configuration is [briskapi/archive.json](briskapi/archive.json): bucket
`brisk-recordings-honvl-tokyo` in Tokyo (`ap-northeast-1`), the archive's function URL
(`api_url`) and, once the cloud API is deployed, its function URL (`query_url`; the client
raises `BriskError` for a query without one). Nothing in the bucket is public but `samples/`
(the legacy wire-format sample capture): each owner reaches its own uploads through the
functions only.
Deployment is described in [infra/README.md](infra/README.md).

### Identity

There is no registration. On first use the client makes a random 128-bit secret,
`brisk-id-v1-` followed by 26 lowercase base32 characters (`briskapi/identity.py`), or takes
`BRISK_IDENTITY`. It keeps two copies: one in the system's credential store (service
`briskapi-archive-identity`, through `passkey.py`'s Keychain and Credential Manager code)
and one in an owner-only file, `identity.json` in the config directory. Reading repairs a
missing copy from the other, refuses copies that differ, and never overwrites an unreadable
file (it is set aside as `identity.json.unreadable`). Every
API request but a ticket's status sends it as `Authorization: Bearer <secret>`, over TLS only:
the client refuses any URL but `https` (or plain HTTP to localhost, for a local service) and
never forwards the header on a redirect (`cli.request_json`, `cli.secure`). The
service derives the **owner ID**, the first 32 hex digits of
SHA-256("brisk-owner-v1:" + secret) (`schema.owner`), and keeps nothing else: no account,
no stored key, nothing to look up. Everything an owner uploads is filed under
`archive/OWNER/` or `timing/OWNER/`, and listings, downloads, deletions and restores are
scoped to the caller's owner ID. Owner IDs can't be reversed to secrets, so they may appear
in prefixes, quotas and logs. Outside the owners' prefixes are the archive's single copy of
a demo replay's market tables (below), which holds no owner's data, and the cloud API's
`bars/OWNER/` (bars built from the owner's own recordings) and `results/OWNER/` (see
[Cloud API](#cloud-api)). The cloud API takes the same secret as its key.

A secret is a bearer credential: whoever holds it acts as that owner. Its **recovery code**
is the same 26 characters in upper case plus two checksum characters (base32 of SHA-256 of
the body), in groups of four; reading it ignores case, spaces and dashes, takes 0, 1 and 8
for O, I and B, and refuses a code whose checksum fails. `brisk identity --recovery-code`
prints it, `--recover CODE` saves it on another computer (`--replace` over a different
identity) and `--export` prints the secret itself. It is never registered, so nothing can
recover a lost secret without its code. A signature scheme would keep the secret on the client entirely,
but needs Ed25519 or similar on both ends, which Python's standard library lacks; the
bearer secret needs no dependency.

The API (`archive_service.py`), JSON with an `action`:

| Action | Fields | Returns |
| --- | --- | --- |
| `upload` | `manifest`, `part` | A ticket and an S3 POST policy bound to `incoming/TICKET/recording.zip`, the exact size of what is sent and encryption. `part` is null for the whole recording (the zip of its four tables), or `{sha256, bytes}` of the zip of its timing table alone, which only a demo or synthetic replay may send. A timing-only upload is accepted once the archive holds the replay's market tables; until then the answer is HTTP 409 (`send`: `recording`), with no ticket issued or counted, and the client sends the whole recording. The owner ID waits in `incoming/TICKET/owner.json` for ingest, and a `part` in `part.json`. |
| `timing` | `report` | The stored key. |
| `list` | `kind` (`recordings`, `timing` or `deleted`), `date`, `after` | A page of the owner's manifests (50) or reports (500), and the key to continue after. `deleted` lists what the owner deleted and can still restore: recordings (`prefix`, `manifest`) and reports (`key`, `report`), each with `deleted_at` and `restore_until`, up to 200 and without paging (`after` must be null). |
| `download` | `prefix` | The manifest and a 15-minute presigned URL for each of the four tables (`files`), for the owner's own prefix only. A demo recording's market tables are the archive's copy under `reference/`. |
| `delete` | `prefix` (a recording's) or a timing key | The current objects deleted, the manifest first, so a listing never shows half a recording. Without a version ID the versioned bucket keeps them as noncurrent versions, which `restore` can bring back for 30 days and the lifecycle removes after 31; the execution role can't delete archive or timing versions. A replay's `reference/` tables are not touched. The owner's bars of a live recording's trading day are then rebuilt without it. |
| `restore` | `prefix` (a recording's) or a timing key | `{"status": "restored", "restored": N}`: each key deleted in the last `RESTORE_DAYS` (30) days is copied back from its newest kept version (`copy_object`), the manifest last. Not found when nothing is restorable. The owner's bars of a restored live recording's trading day are rebuilt with it. See [Deleting, restoring and retention](#deleting-restoring-and-retention). |

`GET /?ticket=UUID` returns a ticket's status; the unguessable ticket is its credential.

Every request, a ticket's status included, first counts against its source address, 120 a
minute (`REQUESTS`), before its identity is read. Identities are free to make and any
well-formed one is accepted, so an address, not an identity, is what bounds guessing: with
2^128 secrets, finding one that has uploads is hopeless at any rate, and the limit bounds what
trying costs. Addresses count as keyed hashes (`address`), an IPv6 address as its /64 (one
connection's usual block, so that moving within it does not reset a limit). Deletes and
restores, each of which may rebuild a day of the owner's bars, are limited to 60 an hour per
address and per owner (`CHANGES`). A request over any limit gets HTTP 429.

### Integrity

The client is open source, so the service validates every upload. Demo and
synthetic content must match a reference; broker live content has no reference and
is uploader-declared:

- **Reference replay.** Market content must equal the pinned demo replay.
  [briskapi/references/historical_mock.json](briskapi/references/historical_mock.json) holds a
  truncated SHA-256 chain per security (master entry plus every update and the
  batch it arrived in) and a hash of the batch clock timeline; it contains no
  market data. Any subset of securities is accepted, but only complete replays.
  `tools/brisk_mock/build_reference.py` regenerates it after a re-audited asset pin.
  This comparison applies only to demo/synthetic sources, not to the broker ones (`sbi_live`, ...).
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
  Broker quotes use null
  replay lateness. Its end frame count includes pre-bootstrap catch-up frames,
  so it can exceed the number of emitted quote batches. Broker packaging removes
  connection/decoder diagnostics and keeps only the site's fixed transport metadata,
  `{"kind":"sbi_websocket","origin":"https://sbi.brisk.jp/"}` for SBI
  (`eshiten_websocket` and `https://next.e-shiten.brisk.jp/` for e-shiten, and so on: `BROKER_TRANSPORT`).
  The service rejects any additional transport fields.
- **Verified tables, no uploaded zip.** The service stores the uploaded tables
  once verified, each as its own object. The zip that carried them is never kept, so
  its comments, extra fields and timestamps never reach the archive, and the
  manifest it stores hashes the service's own deterministic zip of the four tables as
  stored. The tables themselves are not rewritten: what
  table verification can't see (Parquet statistics, unused dictionary entries,
  encoder choices in pages) is the one place an upload could keep a few bytes of
  its own. For a demo replay's market tables, the first verified upload's bytes become the
  archive's copy.
  `synthetic_test` accepts only the fixed one-security probe in `briskapi/schema.py`.
- **A timing table alone is verified as a whole recording.** When an upload sends only
  its timing table, the service fetches the archive's copy of the market tables,
  assembles the four, and runs the same checks as for a whole recording (reference
  replay, canonical tables, exact fields, and the summary in the uploaded manifest) before
  storing the timing table. The timing table is therefore checked against the market
  data it describes: batch sequence, clocks and counts.
- **Nothing lingers in staging.** Each upload is deleted right after validation,
  whether accepted or rejected. A ticket admits only its first object version;
  later POSTs with the same upload form are deleted on arrival.
- **Malformed input.** Corrupt zips, unexpected zip members, compressed members and
  unreadable Parquet are rejected like any other invalid upload. Row counts, list
  lengths and declared decoded sizes are checked before data is read.

A client chooses its bounded timing measurements, which cannot be verified. Broker master
names and market values also remain unverified uploader content; exact fields and
canonical encoding do not establish market accuracy. Upload quotas apply to every source.

### Storage and limits

A recording is stored as its four tables (`_parquet.FILES`), not as the zip it arrived
in. Only the staging object `incoming/TICKET/recording.zip` is a zip, and it is deleted
once validated:

```
archive/OWNER/YYYYMMDD/FROM-TO-SHA256/
    data/equity.parquet                      } a demo or synthetic replay keeps these
    data/order_book_depths.parquet           } three in reference/ instead (below)
    data/trades.parquet                      }
    data/custom/BriskBatch.parquet           the timing table: always the recording's own
    files.json                               where each of the four tables is
    manifest.json                            written last: the commit marker
reference/SOURCE/TIMELINE/
    data/equity.parquet  data/order_book_depths.parquet  data/trades.parquet
    complete.json                            written after the tables
timing/OWNER/YYYYMMDD/SHA256.json
bars/OWNER/YYYYMMDD/
    ohlcv-1s.parquet  ohlcv-1m.parquet       the owner's bars of the day, from its live recordings
    ohlcv-1h.parquet  ohlcv-1d.parquet       (derived; see Cloud API)
results/OWNER/UUID.EXT                       a query result too large to return inline
```

- Recordings are content addressed per owner. FROM and TO are the recording's first and
  last source clock (JST, HHMMSS: `schema.recording_name`, matched by `RECORDING_PREFIX`),
  so a day's recordings list in time order, and SHA-256 is that of the service's zip of
  the four tables as stored, the one the stored manifest hashes. Storage writes the
  tables first, then `files.json`, and the immutable manifest last; listings and
  downloads read only committed recordings.
- A demo or synthetic replay's market tables (equity, depths, trades) are the same for
  every recording of it, so the archive keeps them once, under
  `reference/SOURCE/TIMELINE/`, where TIMELINE is the reference's batch-clock hash
  (`reference_prefix`). The first verified replay becomes the copy: its tables are written
  with conditional puts and `complete.json` after them, so a race keeps the first of
  each table and a copy without `complete.json` is not used. A recording of that replay, the first included,
  stores only its timing table, and its `files.json` points the other three into the copy.
  `download` accepts a `files.json` only if it names exactly the four tables, each under
  the recording's own prefix or its source's `reference/` prefix. The role can read and
  write `reference/` but never delete there, and no owner can list it.
- An upload to a replay with a stored copy sends only the timing table: `upload` with
  `part`, the size and SHA-256 of the one-table zip, and a ticket bound to that size.
  Without a copy the service answers 409 and the client (`cli.upload`) sends the whole
  recording; `market=True` sends it regardless. Ingest verifies the assembled recording in
  full either way (see Integrity).
- Upload tickets last 15 minutes and bind the object key, exact size and encryption.
- Uploads are bound to their first S3 object version. Retries of that event are
  safe, and overwriting an upload cannot replace a stored recording.
- Limits: a 64 MiB package, 2 GiB in canonical JSON form and 16 MiB per batch in
  that form. Tickets are limited to four per IP address per hour (stored as a keyed
  hash, never the address), eight per owner per hour, 64 per hour in total and 5 GiB
  of authorized uploads per UTC day, counted by what is sent (a timing table alone,
  once the copy exists), on top of every request's per-address limit (see
  [Identity](#identity)). A complete demo replay is a 41 MB package.
- Ticket metadata in staging expires after two days. Stored data is kept until its
  owner deletes it; what an owner deletes can be restored for 30 days and is removed
  after 31.
- `brisk prune DIR` removes a local package only after finding its SHA-256 in the
  owner's listing and re-hashing the local tables. The listing's hash is the service's,
  over the tables as stored, so a demo recording is found only if its market tables are
  byte-identical to the archive's copy.
- `pull` asks `download` for the manifest and one presigned URL per table, and checks
  the manifest and archive identity before fetching any. It downloads the four tables
  (stopping beyond the manifest's byte count), packs them into the deterministic zip and
  checks that zip's size and SHA-256 against the manifest, then the tables, summary and
  reference replay (`inspect_package`). It moves the folder (the tables and the
  manifest; no zip is kept) into place atomically.
- Listings warn and skip invalid/unsupported manifests, including old probes
  with incomplete reference metadata, without hiding S3/network failures.

### Deleting, restoring and retention

`delete` removes the current versions of an owner's objects, the manifest first. The
bucket is versioned, so each object stays as a noncurrent version beneath a delete marker.
The execution role may delete such a version only in staging (`s3:DeleteObjectVersion`),
so nothing the function does, bugs included, purges one under `archive/` or `timing/`:
only the lifecycle does.

- `list` with kind `deleted` and `restore` read the bucket's versions under the
  owner's prefixes (`s3:ListBucketVersions`, `versions()` in the service). A key counts as
  deleted when its latest version is a delete marker, and is restorable when an older
  version is kept and the marker is at most `RESTORE_DAYS` (30) days old. The listing
  reads each manifest or report from its kept version and adds `deleted_at` and
  `restore_until`.
- `restore` copies each such key's newest kept version back to its own key
  (`copy_object` with that version ID, so a new current version), the manifest last, so
  a half-restored recording is never listed. After 30 days it finds nothing.
- The bucket's lifecycle (`deploy.py`: rules `ExpireDeleted-archive` and
  `ExpireDeleted-timing`) expires noncurrent versions under `archive/` and `timing/`
  `DELETED_DAYS` (31) days after they became noncurrent, a day after the restore window,
  and removes delete markers once no version is left beneath them. S3 applies lifecycle
  rules asynchronously, so removal can trail that by a day. Current objects are not
  affected, and `reference/` has no rule, since no owner deletes there.
- A live recording's `delete` and `restore` also rebuild its owner's bars of that trading
  day (`owner_bars` in the service). The role deletes `bars/` objects the same way it
  deletes archive ones (current versions only), and the lifecycle rule `ExpireReplacedBars`
  removes the noncurrent versions of replaced or removed bars after a day.

### Sources

The decoded schema accepts `historical_mock`, `synthetic_test` and the brokers' `sbi_live`,
`eshiten_live`, `matsui_live`, `monex_live` and `smbcnikko_live`. The small, self-authored
`synthetic_test` fixture verifies uploads; filter by source when selecting market data. Uploaded
decoded broker recordings retain their `source` and undergo canonical-field, integrity and continuity checks.
Their market accuracy is uploader-declared; they are not silently treated as
reference-verified demo data. New raw-wire uploads remain unsupported. Objects stored
while the archive was public (before policy version 6) sit outside any owner's prefix and
are unreachable through the API.

## Cloud API

Databento-style queries over an owner's recordings: the records of a dataset, symbols, schema
and time range across days, without downloading recordings. The HTTP interface, what is
decided and the plan for later schemas are in [docs/cloud-api.md](docs/cloud-api.md); usage is
in the [README](README.md).

- `query_engine.py` reads the archive bucket (through `Bucket`, a boto3-style client) and
  turns recordings into records (with `briskapi/records.py`, which `briskapi.Live` shares)
  and bars.
- `query_service.py` is the Lambda function URL `brisk-recorder-query`: authentication,
  endpoints, quotas and delivery of results.
- `briskapi/_historical.py` is the client (`Historical`, `DataStore` and `brisk query`).

The engine and the service are server side, like `archive_service.py`, which imports the
engine to keep owners' bars in step: the wheel ships only `briskapi/`, and `package()` in
`deploy.py` puts them, with `briskapi/schema.py`, `_parquet.py` and `records.py`, in the one
zip both functions run. `Historical`'s OHLCV bars therefore come only from the cloud;
`briskapi.Live` builds its own from the stream (see [Live gateway](#live-gateway)).

### Datasets and requests

`XTKS.BRISK` (`publisher_id` 1) is an owner's live recordings, from any of the five brokers (`LIVE` in `query_engine.py`):
any broker's BRiSK is the same app and stream. `XTKS.BRISKDEMO` (`publisher_id` 2) is the
public demo, read from the archive's one copy of its market tables under
`reference/historical_mock/TIMELINE/` (the same for every identity; HTTP 404 until a verified
whole demo upload, or `deploy.py --seed-demo`, has created the copy). `request()` validates
before anything is read: the dataset; the schema (`trades`, `mbp-10`, `definition`,
`imbalance`, `brisk-quotes`, `ohlcv-1s`, `ohlcv-1m`, `ohlcv-1h`, `ohlcv-1d`); `stype_in` raw
symbols only; 1 to 2,000 symbols (JPX codes of four or five characters, 0-9 and A-Z) or
`ALL_SYMBOLS`; at most 31 JST trading dates; a positive `limit`. A date is that JST trading
day's midnight, a time without a zone is JST, an integer of ten or more digits is UTC
nanoseconds, and `end` is exclusive and defaults to the end of the start's trading day (so an
`end` date excludes that day). `instrument_id` comes from the code, because a recording's
`issue_id` only indexes that day's master: digits-only codes are their number, others are
100,000 plus the code's base-36 value (`167A` is 154,694).

### Records

`recordings()` lists `archive/OWNER/DATE/` for each trading date a request touches, reads each
manifest (its summary gives the source, the first and last source clock and the codes) and
`files.json`, and keeps live sources. `Bucket.parquet` hands pyarrow a `ParquetFile` over a
file object that fetches 4 MiB blocks by ranged GET as they are asked for, so only the blocks
holding the footer and the needed row groups are read. `_rows` skips a row group whose
`issue_id` range (tables are sorted by issue) or `ts_event` range, from its statistics, cannot
match, then filters the rest exactly. Each schema is a projection:

| Schema | From | Mapping |
| --- | --- | --- |
| `trades` | trades | `action` T; `side` B for a BUY aggressor, A for SELL, else N; `price` (float64, yen), `size`, `sequence` (the frame) and `trade_id` |
| `mbp-10` | depths | One row per quote update with both books flattened to `bid_px_00`, `bid_sz_00`, `bid_ct_00` … `ask_ct_09` (null past a side's last level). BRiSK gives each update's book, not the order event that changed it, so `action` and `side` are N, `depth` 0, `price` null and `size` 0; `flags` is Databento's snapshot flag (32) on the bootstrap batch's rows (`seq` 0), else 0 |
| `imbalance` | depths | Per update, an opening (`O`) row where the opening indicative price shows and a closing (`C`) row where the closing one does: `ref_price`, `ind_match_price`, `paired_qty` (indicative volume), `side`, `market_buy_qty`, `market_sell_qty`, `market_imbalance_qty` and `_side` (buy minus sell), `bid_total_qty` and `ask_total_qty` (the board totals; the closing ones for `C`), the day's limit prices as collars and `auction_time` (special quote time) |
| `definition` | equity | One row per security per recording, stamped JST midnight of its trading day and only when the request reaches that day: `raw_symbol`, `exchange` XTKS, `currency` JPY, lot size, tick increment, limits, base price, name, issue type, tick type |
| `brisk-quotes` | depths | Every column the update has except `identifier`, `ts_init`, `issue_id` and `seq`; decimals become float64 |
| `ohlcv-*` | trades | Below |

Every schema but bars starts with `ts_recv`, `ts_event`, `publisher_id` and `instrument_id`
and ends with `symbol`. `ts_event` is BRiSK's stamp. `ts_recv` is the uploader's receipt
time: `received_unix_ms` of the update's batch, joined by `seq` (milliseconds; `ts_event`
where there is none), and `ts_event` for the demo, which has no receipt clock of its own. A
request filters and sorts on `ts_event`, not on `ts_recv` as Databento does: overlapping
captures of one day agree on `ts_event` and differ in `ts_recv`, so the same range returns the
same rows from any of them.

A day recorded twice (a restart, two computers, two brokers) gives overlapping recordings.
Their rows are merged and deduplicated, keeping the first, by (`instrument_id`, `trade_id`)
for trades, (`instrument_id`, `ts_event`) for definitions, (`instrument_id`, `sequence`,
`auction_type`) for imbalance and (`instrument_id`, `sequence`) for the rest. The frame
numbers are the server's, which should make captures agree; that is not yet confirmed on live
data across brokers. Rows sort by `ts_event`, `instrument_id` and `sequence`, and `limit` cuts
after the sort. With no recording in range, a result has only the common columns and `symbol`.

`encode()` writes Parquet (zstd, `timestamp[ns, UTC]` times), CSV or JSON lines. For CSV and
JSON, `pretty_ts` gives ISO 8601 UTC times with nanoseconds instead of integer nanoseconds,
`pretty_px` yen as numbers instead of integers in units of 1e-9 yen (Databento's fixed point),
`map_symbols` keeps or drops `symbol` and `compression=zstd` compresses (`.zst`); nested
columns are JSON text in CSV.

### Bars

`build_bars` aggregates trades (symbol, `trade_id`, frame, `ts_event`, price, size) into open,
high, low, close and volume per security and interval, for all four lengths. Several sources
are deduplicated by (symbol, `trade_id`), then ordered by symbol, `ts_event`, frame and the
trade's place in its frame, so open and close are the first and last trade. Intervals are
aligned to JST (a day bar runs from JST midnight), a bar's `ts_event` is its start and an
interval without a trade has no bar. `XTKS.BRISKDEMO` bars are built from the demo's trades on
the fly and returned whole.

For `XTKS.BRISK` an owner's bars are built ahead, from its own current live recordings of each
trading day, each trade counted once. `bars/OWNER/YYYYMMDD/` holds the four files (zstd, sorted
by symbol in 65,536-row groups, so a query for a few securities reads only their row groups).
`owner_bars` in the archive service runs after `store` commits a live recording, after `delete`
and after `restore`: `rebuild_bars` reads the trades of the owner's live recordings of that day
and writes all four files, or removes them when none is left. Rebuilds of one owner's day are
not serialized, so a rebuild that finds the owner's keys for the day changed while it ran runs
again (up to three times). Bars are derived data, so a rebuild that fails removes the day's
files (the service prints `Bars for DATE not rebuilt`) rather than keep bars that no longer
match the recordings; queries then build them from the recordings, with the same result.

`covered` keeps only the bars the recordings cover completely: a bar whose span, clipped to
the Tokyo Stock Exchange's trading sessions (`SESSIONS`: 09:00 to 11:30:01 and 12:30 to
15:30:01 JST, half open; the extra second holds the closing auction's prints), lies inside the
merged clock ranges (first to last source clock) of the owner's recordings that hold that code.
A bar that touches no session is judged on its whole span. So a one-second bar needs its second
recorded, and an hour or day bar needs the whole of its in-session part. Securities held by the
same recordings share their windows, so it is one vectorized pass per such group (usually one),
not one per security.

No other owner's recordings enter an owner's bars, and nothing is lost by that: within its
coverage an owner's own recordings hold every trade, since `validate_batches` refuses a
recording whose trades do not account for every change in a security's cumulative volume and
turnover between its quotes. Others' recordings could only disagree there. 1.0.0 built one set
of shared bars per day from every owner's recordings and cut each owner's to its coverage, which
a made-up recording (it passes validation: nothing ties a broker recording to the real market)
defeated twice: two batches at 09:00 and 15:30 claimed a whole day's coverage and received
everyone's bars, and a made-up price for a real print, from an owner ID ground to sort first,
won deduplication and replaced the real one in everyone's. `test_query.py` keeps both
(`test_no_upload_reveals_or_changes_another_owners_bars`).

Speed, on a day of 1,000,000 trades across 3,774 securities (measured locally, without S3
latency): a rebuild takes about 0.8 s; a query reads one-minute bars in about 1 ms for one
security and 60 ms for every security (one-second bars: 2 ms and 140 ms), against about 0.5 s
to build them from the recording per request. 1.0.0's per-security `covered` took about 25 s
over such a day's four bar sizes, at query time.

### Service

`query_service.py` handles a request in order. The path must be `/v0/NAME` with a known NAME.
Parameters come from the query string and, for POST, the form body (at most 64 KiB; the body
wins). The key is the basic-auth username (the password is ignored) or a Bearer token, and
`schema.owner` turns it into the owner ID. There is no key registry, so any well-formed
identity authenticates: a missing or malformed key is HTTP 400, not 401, and an identity with
no recordings gets empty tables (HTTP 404 from the dates endpoints). Quotas use the archive's
DynamoDB table and `archive.quota`: first `query-ip-ADDRESS` (120 requests a minute, keyed as
`archive.address` does, before the key is read, so requests without a valid key count too),
then `query-OWNER` (60 requests a minute) before the endpoint runs, and on the encoded result's
size `query-bytes-OWNER` (2 GiB per UTC day), `query-bytes-ip-ADDRESS` (4 GiB) and
`query-bytes-all` (20 GiB for everyone); over any is HTTP 429. Errors are `{"detail": ...}` with 400 (invalid input), 404 (no data, or no demo
copy) or 429.

A result of up to 4 MiB (`INLINE`: a function URL returns 6 MB, and base64 adds a third) is
the response body, with `x-record-count`: text for CSV and JSON, base64 for Parquet. A larger
one is written to `results/OWNER/UUID.EXT` and the response is a 303 to a 15-minute presigned
URL; the lifecycle removes it after a day. The endpoints are Databento's:
`timeseries.get_range`; `metadata.list_datasets`, `list_schemas`, `list_fields` (names only),
`list_publishers`, `get_dataset_range` (the first and last date with data) and
`get_dataset_condition` (each date `available` or `missing` with the merged clock windows
recorded, JST, up to a year); `get_record_count` (runs the query and counts its rows) and
`get_cost` (always 0). The function prints nothing.

### Client

`Historical(key=None, gateway=None)` takes the key from `identity.ensure()` (warning when it makes one) and the gateway
from `query_url`. `timeseries.get_range` posts a form, asks for Parquet and returns a
`DataStore` with databento.DBNStore's conversions (`to_df()` indexed by `ts_recv`, or by `ts_event` where there
is none, with `price_type`, `pretty_ts`, `map_symbols`, `tz` and `count`; `to_ndarray()`, `to_csv()`,
`to_json()`, `to_parquet()`, `to_file()`, `replay()`, iteration and `len()`). The metadata calls are GETs, except `get_record_count`
and `get_cost`, which post the request. The key goes as the basic-auth username with an empty
password. A 303 is followed by hand with a plain request (`_NoRedirect` stops urllib doing it),
so the key never reaches the result's URL. `brisk query` posts the same `get_range` request
with the chosen encoding (CSV by default; Parquet needs `--output`).

## Development and deployment

Tests and conventions are in [CONTRIBUTING.md](CONTRIBUTING.md). Archive and cloud API
deployment, IAM scope, costs and removal are in [infra/README.md](infra/README.md).
Archive and cloud API changes reach clients only after `infra/deploy.py` runs.
