# briskapi

[English](https://github.com/honvl/BRiSKapi/blob/main/README.md) | [日本語](https://github.com/honvl/BRiSKapi/blob/main/README.ja.md) | [Documentation](https://honvl.github.io/BRiSKapi/)

An unofficial, pybrisk-style Python API and `brisk` command line for BRiSK auction
data. Consume a live feed, query recordings at any point in time, keep them in your own
private archive, and use BRiSK at SBI Securities or Tachibana e-shiten (BRiSK Next)
with your own account, signing in with a passkey if you like. This
is an independent project, not affiliated with or endorsed by BRiSK, Tachibana,
SBI, TSE or JPX.

> **Data sources:**
> - **No account needed:** the public BRiSK Next demo of 27 September 2021 (one
>   pre-open snapshot and the first three minutes after the open), replayed at
>   its recorded pace. This is not live market data.
> - **With your broker's BRiSK (SBI Securities, or Tachibana e-shiten's BRiSK Next):**
>   BRiSK market data (candles, margin, alerts, schedule, watchlist) and an
>   experimental live feed.

## Which API should I use?

| You want | Use | It reads |
| --- | --- | --- |
| Quotes, trades and auctions as they happen | `briskapi.connect()`, then `Ticker`/`Market` | Your live feed |
| A recording you have, offline or in NautilusTrader | `briskapi.load(dir)` or `briskapi.pull(prefix)`, then `Ticker`/`Market`; `brisk export` | That recording, exactly as recorded |
| History across days, or OHLCV bars, without downloading | `briskapi.Historical()`, `brisk query` | Your uploads in the cloud (the demo for everyone) |
| BRiSK's own bars, margin, turnover, lists, events | `Ticker.candles()`, `.margin()`, `Market().turnover()` and the like | Your broker's BRiSK servers (needs a login) |
| What you have uploaded | `briskapi.Archive()`, `brisk list`, `pull`, `delete`, `restore`, `prune` | Your private archive |

Recording isn't on this list: `brisk live`, `brisk record` and their Python forms record as
they run, and what they record is read through the calls above ([Recording](#recording)).

`Ticker.history()` and `.trades()` never go to the network. They read the loaded recording
or the live feed:
- A recording gives exactly the span it covers.
- A live feed needs `history=True`. It then keeps every update since it connected, and trades
  back to the day's start, as far as BRiSK's snapshot restores them.

`Ticker.candles()` is the call that asks your broker's BRiSK. `5m` covers today only; how far
back `1d`, `1w` and `1mo` go is up to BRiSK and not yet known. For bars from your own
recordings, across days, use `Historical` (`ohlcv-1s` to `ohlcv-1d`).

The local calls and the cloud API name the same data differently (`indicative_price` locally,
`ind_match_price` in the cloud, for example). [docs/data-format.md](https://github.com/honvl/BRiSKapi/blob/main/docs/data-format.md#local-and-cloud-names)
maps one to the other.

## Install

Python 3.12+. Live feeds and recording also need Node 22+, because BRiSK's own
decoder is a WebAssembly module that runs under Node.

```sh
pip install 'briskapi[pandas]'      # `import briskapi` and the `brisk` command
```

BRiSK's decoder and demo data are downloaded at runtime; the package doesn't
include them. Optional prebuilt Rust tools (`brisk_quote_ingest`,
`brisk_recording`) for Linux, macOS and Windows are attached to each
[GitHub release](https://github.com/honvl/BRiSKapi/releases). To work from source,
clone the repository and run `pip install -e '.[pandas]'`.

## Recording

Recording isn't a third API. The live calls record as they run, and `brisk live` and
`brisk record` are their command-line form, with the same code, options and upload choice.

| Command line | Python | Records | Kept on disk | Uploaded |
| --- | --- | --- | --- | --- |
| `brisk live --sbi` or `--site e-shiten` | `brisk.connect()` | Your broker's live feed, one recording per trading day, until you stop it | Each finished day, with `--output DIR` (`output=`) | Each finished day, unless you opt out |
| `brisk live --web` | `briskapi.connect(web=True)` | The demo replay, printed as JSON lines | With `--output DIR` | If the replay completes |
| `brisk record --web --output DIR` | `briskapi.record(DIR, web=True)` | The demo replay | Always, in `DIR` | If the replay completes |

What you record is then read through the other two APIs:
- **On your computer:** `briskapi.load("days/sbi_live_20261013_085800-153512")`, then `Ticker`
  and `Market`, exactly as recorded; `brisk export` for NautilusTrader or JSONL; or the Parquet
  tables themselves ([data format](https://github.com/honvl/BRiSKapi/blob/main/docs/data-format.md)).
  `brisk pull` (`briskapi.pull()`) brings an uploaded recording back to disk.
- **In the cloud:** `briskapi.Historical()` or `brisk query XTKS.BRISK` reads every broker day
  you uploaded, merged across days, with OHLCV bars. Demo recordings aren't in `XTKS.BRISK`;
  everyone can query the same demo as `XTKS.BRISKDEMO`.

Without `--output`, `brisk live` keeps nothing on disk: each finished day goes to your
[archive](#your-private-archive) and its local copy is removed. With uploads off as well
(`--no-upload`, or `--no-upload-market-data` for a broker's market recording), that day isn't
kept anywhere, so add `--output DIR` to keep it. A day is recorded once it ends: stopping
partway keeps none of that day's market data (a broker session still uploads its timing
summary unless you opt out).

## Live feed

```python
import briskapi

feed = briskapi.connect(web=True, codes=["7203", "6758"])   # returns once initial state is in
toyota = briskapi.Ticker("7203")
toyota.quote()        # current quote, updated as each frame arrives
toyota.auction()      # indicative price/volume and market-order imbalance

feed.on_quote(lambda q: print(q["code"], q["indicative_price"]), codes="6758")
for q in feed.quotes("7203"):       # one item per update; ends with the session
    if q["last_price"]:
        print("opened at", q["last_price"], q["time"])
        break

briskapi.Market().imbalances(top=10).to_pandas()
feed.wait()           # or feed.close(); `with briskapi.connect(...) as feed:` also works
```

Options: `web=True` (fetch from the demo site) or `cache=DIR` (a local copy of
the demo assets), `codes`, `speed` (`1` real time, `0` as fast as possible) and
`history=True` (keep updates for `Ticker.history()`). Callbacks and iterators
first receive each security's current quote, then every update in order. A slow
consumer slows the feed instead of losing updates.

## Recordings and your archive

```python
recordings = briskapi.recordings(source="historical_mock")   # yours only; no account needed
if recordings:
    briskapi.pull(recordings[0]["prefix"])      # verify, decode and cache; becomes the default
else:
    briskapi.record("recordings/my-session", web=True)   # record the demo (uploaded unless you opt out)
# Or briskapi.load("recordings/my-session") for a local recording folder (or an older events.jsonl[.gz]).

briskapi.Ticker("7203").quote(at="08:59:59.9999")             # available pre-open quote, in JST
briskapi.Ticker("7203").history(start="09:00", end="09:01")   # every update in a window
briskapi.Market().snapshot(at="09:00:00").to_pandas()
```

A recording is a folder of Parquet tables laid out like a NautilusTrader v2 catalog:
`data/order_book_depths.parquet` (every quote update: v2's `OrderBookDepth`, then BRiSK's
other quote fields), `data/trades.parquet` (every execution, as v2 `TradeTick`),
`data/equity.parquet` (the securities master, as v2 `Equity`) and
`data/custom/BriskBatch.parquet` (per-frame timing). The complete demo replay takes about
41 MB. pandas, Polars and DuckDB read the tables directly; `brisk export` writes a
NautilusTrader catalog or JSONL. [docs/data-format.md](https://github.com/honvl/BRiSKapi/blob/main/docs/data-format.md)
describes every column, with examples for pandas, Polars and DuckDB.

Time queries use each quote's timestamp. Toyota's first demo quote is at
08:59:59.993551 JST; an earlier query raises `NotFoundError`. `Market().summary()`
returns both `start` and `end`, including for local recordings and live feeds.
Your archive holds only what you uploaded, under your identity: a random key the tool
makes on your computer and never registers (see [Your private archive](#your-private-archive)).
Listings skip invalid or unsupported recordings with a warning. One sample stays a public
download for everyone: a [raw wire capture](https://brisk-recordings-honvl-tokyo.s3.ap-northeast-1.amazonaws.com/samples/20260311/cca870a51e7f96f16009c3709c3f597d314b22ed8922bb3ea7fa1acc67a0f85f/events.jsonl.gz) (2026-03-11, from 12:12:40 JST), in
the legacy `brisk-sbi-wire-jsonl-v1` format the tool no longer reads.

## Your broker's BRiSK

The same client serves SBI Securities' BRiSK ([sbi.brisk.jp](https://sbi.brisk.jp), the
default site) and Tachibana e-shiten's BRiSK Next (its own subsection follows); you need a
BRiSK subscription with the broker. For SBI, log in on sbi.brisk.jp in your browser, then
pass its session cookies: copy them from DevTools, or use `pycookiecheat`'s
`chrome_cookies("https://sbi.brisk.jp")`. Or let `briskapi` sign in with your passkey
([below](#signing-in-with-a-passkey-experimental)).

```python
from briskapi import brisk

brisk.login(cookies={"session_bfaf77a2": "v2.local..."})   # remember=True saves them (owner-only file)
toyota = briskapi.Ticker("7203")
toyota.candles("5m").to_pandas()   # price bars: 5m (today), 1d, 1w or 1mo
toyota.margin(days=30)             # margin balances and stock-lending fees
market = briskapi.Market()
market.turnover()     # turnover and shares outstanding, all stocks
market.lists()        # NK225, recent IPOs, …
market.events()       # basket orders, limit up/down, volume surges
market.schedule()     # trading date, status and session times
market.watchlist()    # your saved codes

feed = brisk.connect(codes=["7203"])        # live (experimental); each finished day uploaded unless you opt out
toyota.quote()                              # same calls as any feed
# upload_market_data=False uploads no market recordings, upload=False nothing; output="days" keeps each day
```

A broker feed records continuously, one recording per trading day, and keeps running across
days. When the site starts serving the next trading date (checked every 10 minutes), or the
server closes the stream after the market, the day ends: it is packaged and uploaded (its
market recording and timing summary, unless you opt out) while the next day records from a
fresh snapshot. `output=DIR` (`brisk.connect(output=...)`, `briskapi.connect(output=...)`,
`brisk live --output DIR`) also keeps each finished day in DIR, in a folder named
`SOURCE_YYYYMMDD_FROM-TO` (the day's first and last source clock, JST HHMMSS), such as
`sbi_live_20261013_085800-153512`. `feed.recordings` lists the finished days (`trading_date`,
`name`, `path`, `uploaded`, `timing_uploaded`), and `on_day=callback` is called for each.
Without `output`, a finished day is kept only in your archive, and with uploads off, nowhere. A
day still open when you stop the feed is not uploaded, only its timing summary.

Results use the conventions below. Errors are `brisk.SessionExpiredError` (log in
again), `briskapi.NotFoundError`, `brisk.RateLimitError` and `brisk.APIError`.
Requests are limited to one per second.

The live feed runs BRiSK's own decoder under Node, downloaded with your session;
no browser is involved. It follows the vendor client's connect sequence: it feeds
the decoder from the first frame, catches the snapshot up to the stream, answers
the server's pings through the decoder and watches the server heartbeat. It hasn't
yet been validated against a live SBI session. The stream is expected to be a plain
binary WebSocket, as in the Next client. Should the server open with Socket.IO
instead, the host joins its `/v2/user` namespace with unverified parameters that
`brisk.connect(profile={...})` can set. See what the server answers with
`trace_protocol=True` (every token redacted); anything unexpected stops with an
explicit error rather than a guess. Please report what you see. Live sessions now
get through sign-in, the data endpoints, the decoder download, the stock master and
the snapshot (loaded with the session schedule). The WebSocket upgrade to
`api.brisk.jp` was then answered with HTTP 502; those sessions didn't ask for the
`flex` subprotocol the vendor client asks for, which this version does, and whether
that is enough is not known yet. Your SBI cookies
go only to sbi.brisk.jp.

### Tachibana e-shiten BRiSK Next

e-shiten's BRiSK Next ([next.e-shiten.brisk.jp](https://next.e-shiten.brisk.jp)) is the
same app, so the same client serves it. Log in there in your browser and pass its
cookies with `site="e-shiten"`, or let `briskapi` sign in with your e-shiten passkey
([below](#signing-in-with-a-passkey-experimental)): `brisk enroll --site e-shiten` once,
then `brisk login` before `brisk live --site e-shiten`.

```python
brisk.login(cookies={...}, site="e-shiten")  # or BRISK_ESHITEN_COOKIES; remember=True keeps them apart from SBI's
feed = brisk.connect(codes=["7203"])         # live (experimental)
```

Everything above applies, daily recordings and uploads included (each finished day's market
recording and timing summary); its cookies go only to
next.e-shiten.brisk.jp. e-shiten allows one
BRiSK session at a time: opening it in a second tab or browser has stopped the first
(message 10011), so the feed and BRiSK in your browser can end each other. Before you
record or upload, read what Tachibana's BRiSK Next terms say about storing and reusing
its data.

## Signing in with a passkey (experimental)

Many brokers now have you sign in with a passkey. If yours does, `briskapi` can do the
sign-in for you and keep the BRiSK session cookies, instead of you copying them from a
browser. It works with the BRiSK sites of SBI, Matsui, Monex, SMBC Nikko and Tachibana e-shiten,
or any broker you add. It needs Chrome, Node 22+ and a safe place for the passkey: the macOS Keychain or Windows
Credential Manager (on Linux, an owner-only file you opt into). It runs on macOS and Windows (run it
from PowerShell or `cmd` so it uses Windows' own Chrome and Node, not WSL's) and is also tested on Linux.

### How it works

A passkey is a pair of keys. The broker keeps the public half. The private half normally lives
in your phone, laptop or password manager, where a fingerprint or PIN lets it sign a challenge
the broker sends, which proves it is you. `briskapi` borrows that arrangement. It starts its own
Chrome, which has a *virtual authenticator*: software that plays the part of your phone and signs
without asking. The broker's site cannot tell the difference.

1. **Once, `brisk enroll`.** You pick your broker. A separate Chrome window opens on its login
   page (a temporary profile, not your everyday Chrome). Sign in as you normally would, then
   register a new passkey in the broker's security settings, just as you would for a new phone.
   The virtual authenticator receives it. When the site says it is registered, press Enter in
   the terminal: `briskapi` saves the passkey and your broker choice in the macOS Keychain or Windows Credential
   Manager, and Chrome closes.
   At e-shiten, `enroll` first asks for your user ID, because its passkey form wants the ID before
   the passkey; it is saved with the passkey. The window opens at e-shiten's usual sign-in (user ID,
   PIN and phone-number authentication), and the passkey is registered on the screen shown after
   signing in or under お客様情報 › 設定情報 › パスキー認証.
2. **Each session, `brisk login`.** The same Chrome opens with the saved passkey loaded, goes to the
   login page and presses the passkey button, and the virtual authenticator signs. Open BRiSK from
   the broker's site in that window (or pass `--launch-url` to have it opened for you).
   `briskapi` reads the BRiSK session cookies, saves them and closes Chrome.
3. **Then use it.** For SBI and e-shiten, `brisk live --sbi` or `brisk live --site e-shiten` and the
   `briskapi.brisk` API use the saved cookies. For the other brokers `briskapi` has no data client
   yet, so the cookies are only saved (under `~/.config/brisk/cookies/`) for your own use.

Your existing passkeys on your phone or in a password manager are not touched. The broker simply
lists one more passkey, which you can delete in its security settings.

### What to know

- **The saved passkey is a credential.** Whoever has it can sign in to your broker account with no
  further check. It is kept only in the Keychain or Credential Manager (`BRISK_PASSKEY_STORE=file` keeps it in an
  owner-only file instead, which is weaker; on Windows that file's privacy comes from your user
  profile folder, which only you and administrators can read), never leaves your machine and is never shown in a
  log. For e-shiten your user ID is kept with it, in the same place. `brisk forget` deletes it here; remove
  it at the broker too.
- **It is experimental.** Each broker's login page, passkey button text and BRiSK launch come from
  its public pages, and none has been tried against a live account. Override them with
  `--login-url`, `--passkey-button` and `--launch-url`, or in `sites.json` below, and please report
  what fails.
- **A broker may refuse it.** Chrome tells the site it is being automated. Check that your
  broker's terms allow this before relying on it.

### Commands

```sh
brisk sites                    # the brokers you can pick, and where their BRiSK lives
brisk enroll [--site matsui]   # once; you are asked to choose a broker if you don't say
brisk login                    # each session; the broker is the one you enrolled with
brisk forget                   # delete the saved passkey
```

From Python: `passkey.enroll(site="matsui")` (for e-shiten, `passkey.enroll(site="e-shiten", user_id="...")`)
and `signin = passkey.login()`, where `signin.cookies` are the BRiSK cookies and `signin.client` is the data
client for sites that have one.

To add a broker, or correct a built-in one, edit `~/.config/brisk/sites.json`:

```json
{"sites": [{"id": "mybroker", "name": "My Broker", "login_url": "https://broker.example/login",
            "cookie_host": "mybroker.brisk.jp", "passkey_button": "Sign in with a passkey"}]}
```

Other keys: `cookie_prefix`, `launch_url`, `enroll_url` (where `enroll` starts, if the first sign-in
is on another page than the passkey one) and `user_field` (a CSS selector for a user ID box that must
be filled before the passkey control works; `enroll` then asks for the ID).

How it works inside: [ARCHITECTURE.md](https://github.com/honvl/BRiSKapi/blob/main/ARCHITECTURE.md#passkey-sign-in).

## API reference

| Call | Returns |
| --- | --- |
| `briskapi.connect(...)` | Live `Feed`; becomes the default source |
| `Ticker(code).info()` | Name, lot size, tick type, base price and daily limits |
| `Ticker(code).quote(at=None)` | Bid/ask with their quantities and orders, the ten best book levels per side (`bids`, `asks`) and of the closing-condition orders (`closing_bids`, `closing_asks`), the board's order totals, indicative price/volume, market-order and closing quantities, last trade, turnover, issue status and `special_quote` (side, price, when its timer ends) |
| `Ticker(code).auction(at=None, closing=False)` | The auction's indicative state, market orders and board totals with `market_order_imbalance` (market buy minus sell); `closing=True` for the closing auction |
| `Ticker(code).history(start, end)` | Every update in order, from the loaded recording or feed (no network) |
| `Ticker(code).trades(start, end)` | Every execution: time, price, shares and the initiating side (`buy`, `sell`; None for most opening prints), from the loaded recording or feed |
| `Market().stocks()` | Master for every security |
| `Market().snapshot(at=None)` | Every security's quote |
| `Market().imbalances(at=None, top=None, closing=False)` | Securities ranked by absolute market-order imbalance (or the closing auction's) |
| `Market().special_quotes(at=None)` | Securities in a special quote (特別気配): side, price, when its timer ends |
| `Market().trades(start, end)` | Every security's executions in time order |
| `Market().frames()` | A recording's batches: clocks, local timing, quote and trade counts |
| `Market().summary()` | Source, date, coverage and clock range |
| `Feed.quotes(codes)` / `Feed.on_quote(fn, codes)` | Live updates as they arrive |
| `Feed.recordings` | A live feed's finished days: `trading_date`, `name`, `path`, `uploaded`, `timing_uploaded` |
| `briskapi.recordings()` / `.pull()` / `.load()` | Your archive's listing, verified download, local file |
| `briskapi.Archive().upload(dir)` / `.delete(prefix)` / `.deleted()` / `.restore(prefix)` / `.timing()` / `.owner` | Upload, delete, what you deleted in the last 30 days (with `deleted_at`, `restore_until`), bring one back, your timing reports, your owner ID |
| `briskapi.record(output, web=True, ...)` | A recording of the demo, uploaded per your choice |
| `briskapi.consent(...)` | Your upload choice |
| `briskapi.Historical()` | The cloud API's client, Databento-style; your archive identity is the API key ([below](#querying-your-archive-in-the-cloud)) |
| `Historical().timeseries.get_range(dataset, start, end=None, symbols='ALL_SYMBOLS', schema='trades', limit=None, path=None)` | A store of records from `XTKS.BRISK` (your recordings) or `XTKS.BRISKDEMO`: Databento's conversions: `to_df()` (indexed by `ts_recv`, or `ts_event` for bars), `to_ndarray()`, `to_csv(path)`, `to_json(path)`, `to_parquet(path)`, `to_file(path)`, `replay(callback)`, iteration, `len()` |
| `Historical().metadata.list_datasets()` / `.list_schemas(dataset)` / `.list_fields(schema)` / `.list_publishers()` / `.get_dataset_range(dataset)` | The catalogue; a dataset's first and last date with data |
| `Historical().metadata.get_dataset_condition(dataset, start_date, end_date)` | Each date available or missing, and the clock windows (JST) your recordings cover |
| `Historical().metadata.get_record_count(...)` / `.get_cost(...)` | How many records a request returns; its cost, always 0 (free, with quotas) |
| `Ticker(code).candles(interval)` / `.margin(days)` | BRiSK's own price bars from your broker (SBI, e-shiten): `5m` today only, how far back `1d`/`1w`/`1mo` go is up to BRiSK; margin balances and lending fees |
| `Market().turnover()` / `.lists()` / `.events()` / `.schedule()` / `.watchlist()` | Broker BRiSK market data |
| `briskapi.brisk.login(site=...)` / `.connect()` | SBI (default) or e-shiten BRiSK session and live feed |
| `briskapi.passkey.enroll()` / `.login()` / `.forget()` | Passkey sign-in to your broker's BRiSK |

Prices are yen floats, with `None` for the vendor's zero "unavailable" value.
Times are JST `datetime`s on the trading date. Quantities are shares; side, flag
and status codes are raw vendor values. `raw=True` returns vendor fields
(`*_price10` in tenths of a yen, `*_us` in microseconds since JST midnight).
Tabular results are lists of dicts with `.to_pandas()`. Errors are
`briskapi.BriskError` and `briskapi.NotFoundError`. The first whole-market query
reads a recording's tables into memory (about two seconds for the complete demo);
one security's history is read on its own.

## Command line

```sh
brisk live --web --codes 7203,6758          # one JSON object per quote update (--raw for vendor fields)
brisk sites | enroll | login | forget       # sign in to your broker's BRiSK with a passkey through Chrome (experimental)
brisk live --sbi --codes 7203               # BRiSK at SBI Securities; cookies from BRISK_SBI_COOKIES (JSON) or `brisk login`; --trace-protocol shows the handshake, wire details in BRISK_PROFILE
brisk live --site e-shiten --codes 7203     # Tachibana e-shiten BRiSK Next; cookies from BRISK_ESHITEN_COOKIES (JSON) or `brisk login`
brisk live --sbi --output days              # runs day after day; keeps each finished day in days/ (e.g. sbi_live_20261013_085800-153512) and prints a {"day": {...}} line on stderr
brisk record --web --output recordings/s1   # record a replay (uploaded unless you opt out)
brisk list --date 20210927 --source historical_mock   # your recordings (--timing: your timing reports; --deleted: what you deleted in the last 30 days)
brisk pull PREFIX --output recordings/downloaded   # use a prefix returned by brisk list
brisk delete PREFIX                         # remove one of your uploads from your archive (restorable for 30 days)
brisk restore PREFIX                        # bring back an upload deleted in the last 30 days (also a timing report's key)
brisk prune recordings/s1                   # free disk: remove a local recording your archive holds
brisk export recordings/s1 --nautilus catalog      # a NautilusTrader v2 catalog (or --jsonl FILE)
brisk query XTKS.BRISK --schema ohlcv-1m --symbols 7203 --start 2026-10-13   # the cloud API: CSV to stdout (--encoding json|parquet, --output FILE, --end, --limit)
brisk consent [--accept | --revoke]         # show or change automatic uploads (--no-upload: this run only)
brisk upload recordings/s1                  # retry uploading a recording
brisk identity [--recovery-code | --recover CODE]  # your owner ID; its recovery code; use it on another computer
```

Each command has `--help`. `pull` verifies everything before writing and never
overwrites an existing folder.

Each command is the command-line form of a Python call:

| Command | Python |
| --- | --- |
| `brisk live --web` (or `--cache DIR`) | `briskapi.connect(web=True)` |
| `brisk live --sbi`, `--site e-shiten` | `brisk.login()`, then `brisk.connect()` |
| `brisk record` | `briskapi.record()` |
| `brisk query` | `briskapi.Historical().timeseries.get_range()` |
| `brisk list` (`--deleted`, `--timing`) | `briskapi.recordings()` (`Archive().deleted()`, `.timing()`) |
| `brisk pull`, `upload`, `delete`, `restore`, `prune` | `briskapi.pull()`, `Archive().upload()`, `.delete()`, `.restore()`, `.prune()` |
| `brisk consent` | `briskapi.consent()` |
| `brisk enroll`, `login`, `forget` | `briskapi.passkey.enroll()`, `.login()`, `.forget()` |
| `brisk export`, `brisk identity` | Command line only |

`brisk live --sbi` and `--site e-shiten` run until you stop them, one recording per trading
day. Unless you opt out, they upload each finished day's market recording and timing summary
without asking; `--no-upload-market-data` stops uploading the recordings and `--no-upload` both
(add `--output DIR` to keep them on disk). `brisk list --source sbi_live` (or `eshiten_live`) lists your broker recordings.

## Your private archive

Uploading gives you a free, off-computer copy of every complete session, checked on arrival
(a complete demo replay must match the reference). Once a recording is in your archive,
`brisk prune DIR` deletes the local copy (after checking its SHA-256 against your archive)
and `brisk pull` brings it back, on this computer or any other. Uploading also gives you the
[cloud API](#querying-your-archive-in-the-cloud) and its OHLCV bars, which exist only there.
Each upload is filed under
`archive/OWNER/YYYYMMDD/FROM-TO-SHA256` (the prefix `brisk list` returns), FROM and TO being
the recording's first and last source clock (JST, HHMMSS), so a day's recordings sort by time.

Uploads are private: only you can list, download or delete them. There is no account. The
first time it needs one, the tool makes your **identity**, a random secret, and the archive
files your uploads under a one-way hash of it, your owner ID. The secret is kept twice, in
your system's credential store (the macOS Keychain or Windows Credential Manager) and in an
owner-only file (`~/.config/brisk/identity.json`); each restores the other. When it is made,
the tool shows its **recovery code**, 28 letters and digits: write it down.
`brisk identity --recovery-code` shows it again, and `brisk identity --recover CODE` uses it
on another computer (or set `BRISK_IDENTITY`). Keep it private: whoever holds it is you to
the archive, and without it or a saved copy your uploads can't be reached. The archive's
operator can access what it stores, as with any hosted service; see
[PRIVACY.md](https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md).

Uploads are on by default. The first time you record or start a live session from the
command line, the tool shows what is uploaded and asks once (Enter accepts, `n` declines),
and saves your answer. After that, every clean, complete session (or finished day of a live
feed) uploads automatically,
without asking again. Where nothing can ask, in the Python API or a command run without a
terminal, sessions upload by default and the tool says so once per process (a warning in
Python), until you decide with `brisk consent --accept` or `--revoke`
(`briskapi.consent(accept=True)` or `revoke=True`).

- **What a demo session uploads:** local timing measurements (including your computer's
  clock, which shows when you recorded). A demo replay's market data is the same for
  everyone, so the archive keeps it once: a demo upload sends only its own timing table
  (about 0.4 MB instead of about 41 MB), the archive joins it to its verified copy of the
  demo's market tables, and `brisk pull` returns the complete recording, verified as before.
  (The very first demo upload to an archive sends the whole recording.) Your IP address is
  used only for rate limits, as a keyed hash.
- **What a broker session (SBI, e-shiten) uploads:** a timing summary (percentiles of
  decode time, data age at receipt and frame spacing, a stall count, the frame count, the
  trading date and the first and last minute; `briskapi.Archive().timing()` lists yours)
  and, once a trading day has ended, that day's decoded market recording: securities master,
  prices, quantities, books, trades, codes and local timing. Cookies, tokens and connection
  diagnostics are never uploaded. The day's trades also give you OHLCV bars in the cloud,
  built from your own recordings only (see [below](#querying-your-archive-in-the-cloud)).
- **Opting out:** `--no-upload` for one run (`upload=False` in Python);
  `--no-upload-market-data` (`upload_market_data=False`) uploads a broker session's timing
  summary but not its market recording (`--output DIR` keeps that); `BRISK_UPLOAD=0` for every run in that environment;
  `brisk consent --revoke` for good.
- **Deleting and restoring:** `brisk delete PREFIX` removes an upload from your archive, so
  you can no longer list or download it. For 30 days you can bring it back:
  `brisk list --deleted` shows what you deleted (with `deleted_at` and `restore_until`) and
  `brisk restore PREFIX` (also a timing report's key) restores it. After 30 days it can no
  longer be restored and is removed.
- Partial replays (`--limit-frames`) and recordings closed before their day ends are never
  uploaded; a broker feed stopped mid-day uploads only its timing summary.

## Querying your archive in the cloud

Uploads also give you a cloud API over your own recordings, shaped like Databento's
historical API (the same names and parameters). Your archive identity is the API key: there
is no signup and no key to create, and `briskapi.Historical()` finds your identity the way
uploads do (and makes one if you have none).

```python
import briskapi

client = briskapi.Historical()
trades = client.timeseries.get_range("XTKS.BRISK", start="2026-10-13", end="2026-10-17",
                                     symbols=["7203"], schema="trades")
trades.to_df()      # a pandas DataFrame indexed by ts_recv; also to_ndarray(), to_csv(), to_json(), to_parquet(), replay()
client.timeseries.get_range("XTKS.BRISK", start="2026-10-13", symbols=["7203"],
                            schema="ohlcv-1m").to_df()     # 1-minute bars (see below)
client.timeseries.get_range("XTKS.BRISKDEMO", start="2021-09-27T08:59", end="2021-09-27T09:03",
                            symbols=["7203"], schema="imbalance")    # the demo, for any identity
client.metadata.get_dataset_condition("XTKS.BRISK", start_date="2026-10-01")   # each date and the clock windows you recorded
```

`get_range(dataset, start, end=None, symbols='ALL_SYMBOLS', schema='trades', limit=None,
path=None)` returns a store; `path` also writes its Parquet file. `client.metadata` has
`list_datasets()`, `list_schemas(dataset)`, `list_fields(schema)`, `list_publishers()`,
`get_dataset_range(dataset)`, `get_dataset_condition(dataset, start_date, end_date)` (each date
available or missing, and the clock windows your recordings cover, in JST),
`get_record_count(...)` and `get_cost(...)`. `get_cost` is always 0: the cloud API is free,
with quotas of 60 requests a minute and 2 GB a day per identity, and 120 requests a minute and
4 GB a day per IP address.

- **Datasets** follow Databento's `VENUE.FEED` naming (XTKS is the Tokyo Stock Exchange).
  `XTKS.BRISK` is your own live recordings, whichever broker's BRiSK they came through.
  `XTKS.BRISKDEMO` is the public demo (2021-09-27), open to every identity.
- **Schemas:** `trades`; `mbp-10` (ten levels per side; BRiSK gives books, not order events,
  so `action` and `side` are `N`); `definition`; `imbalance` (opening and closing auction
  state: indicative price and volume, market-order imbalance); `brisk-quotes` (every BRiSK
  quote field); and the OHLCV bars `ohlcv-1s`, `ohlcv-1m`, `ohlcv-1h` and `ohlcv-1d`.
  `list_fields(schema)` lists a schema's columns.
- **Times:** a date means that JST trading day, a time without a zone is JST and an integer is
  UTC nanoseconds. `end` is exclusive and defaults to the end of the start's trading day. A
  request covers at most 31 days and 2,000 symbols.
- **Symbols** are JPX codes. `instrument_id` is fixed per code: digits-only codes are their
  number (7203), others are 100,000 plus the code's base-36 value (`167A` is 154694).

**OHLCV bars come only from the cloud.** The archive builds your bars for each trading day
from your own current live recordings of that day, counting each trade once, whenever you
upload, delete or restore one, so a query reads them ready-made. They hold the bars your
recordings cover: the security, and the bar's whole span within the trading sessions (09:00 to
11:30 and 12:30 to 15:30 JST). A recording from 08:55 to 11:31, for example, gets the morning's
1-minute and 1-hour bars but not the day bar. So uploading is how you get bars. No one else's
uploads enter yours, and yours enter no one else's. (`XTKS.BRISKDEMO`'s bars are built from
the demo's own trades.) See [PRIVACY.md](https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md)
(policy version 7).
`Ticker.candles()` is something else: BRiSK's own bars from your broker, which need a broker
login.

On the command line, `brisk query` writes CSV to stdout. `--encoding json` or `parquet` changes
that (Parquet needs `--output FILE`), `--output FILE` writes a file instead, and `--end` and
`--limit` bound the request:

```sh
brisk query XTKS.BRISK --schema ohlcv-1m --symbols 7203 --start 2026-10-13 > toyota-1m.csv
```

The HTTP API behind both is described in
[docs/cloud-api.md](https://github.com/honvl/BRiSKapi/blob/main/docs/cloud-api.md).

## More documentation

The [documentation site](https://honvl.github.io/BRiSKapi/) has these documents and an API reference with
an example of each call and what it returns.

- [docs/data-format.md](https://github.com/honvl/BRiSKapi/blob/main/docs/data-format.md): every column of recordings and cloud API records, and how to read them
- [docs/cloud-api.md](https://github.com/honvl/BRiSKapi/blob/main/docs/cloud-api.md): how the cloud API works
- [ARCHITECTURE.md](https://github.com/honvl/BRiSKapi/blob/main/ARCHITECTURE.md): how it works, data format, archive integrity and limits
- [PRIVACY.md](https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md): privacy policy
- [CONTRIBUTING.md](https://github.com/honvl/BRiSKapi/blob/main/CONTRIBUTING.md): development, tests and releases
- [tools/brisk_mock/README.md](https://github.com/honvl/BRiSKapi/blob/main/tools/brisk_mock/README.md): field definitions, timing and latency, checking the SBI master layout, and the optional Rust collector
- [infra/README.md](https://github.com/honvl/BRiSKapi/blob/main/infra/README.md): deploying an archive
- [THIRD_PARTY.md](https://github.com/honvl/BRiSKapi/blob/main/THIRD_PARTY.md): decoder, data and pybrisk attribution

Software is MIT licensed.
