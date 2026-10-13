# Privacy policy

Policy version 6, effective 13 October 2026. Applies to the `brisk` Python API and
CLI in this repository and the private recording archive they upload to.

## Your private archive

Uploads are private. Each one is filed under your **identity**: a random secret the tool
makes on your computer the first time it needs one. It is kept in your system's credential
store (the macOS Keychain or Windows Credential Manager; elsewhere, or with
`BRISK_IDENTITY_STORE=file`, only the file) and in `~/.config/brisk/identity.json` (or
`$XDG_CONFIG_HOME/brisk/`), readable only by you; each copy restores the other. There is no
registration, account, email address or password. The archive knows you only by your
**owner ID**, a one-way hash of the secret, and lists, downloads and deletes only the
uploads of the owner ID a request proves it holds. Nobody else can list or download them.
The bucket's only public objects are the samples under `samples/`, a copy of the legacy
wire-format sample capture.

- Whoever holds the secret is you to the archive. Keep it private. It also reads as a
  28-character **recovery code**, shown when the identity is made and again with
  `brisk identity --recovery-code`; `brisk identity --recover CODE` uses it on another
  computer, and `BRISK_IDENTITY` supplies the secret to a process instead.
- Losing every copy and the recovery code loses access to what that identity uploaded.
  The uploads remain stored until the operator removes them.
- `brisk delete PREFIX` (or `briskapi.Archive().delete(prefix)`) removes a recording or
  timing report from your archive: you can no longer list or download it. For **30 days**
  you can bring it back: `brisk list --deleted` shows what you deleted and until when, and
  `brisk restore PREFIX` (or `briskapi.Archive().restore(prefix)`) restores it. After that
  it can't be restored, and the storage removes it within a day.
- The archive's operator (below) runs the storage and can access what it holds, as with
  any hosted service. The operator does not publish or share it.

## Automatic uploads

Uploads are on by default. The first time you record, package or open a live session from
the command line, the tool shows a short notice of everything described below and asks
once. Pressing Enter accepts, and the choice is saved for future runs. After that, every
clean and complete session is uploaded to your private archive without further prompts: demo sessions (`brisk record`, `brisk live`,
`briskapi.connect()`, `briskapi.record()`), and broker sessions (SBI or Tachibana
e-shiten: `brisk live --sbi`, `--site e-shiten`, `brisk.connect()`), which upload a timing
summary and their decoded market recording. A broker session records one trading day after
another until you stop it, and each day is uploaded once it ends (when the site serves the
next trading date, or the server closes the stream). A live session is written to a
temporary file per day while it runs; the file is deleted after that day's upload.

Python API calls never prompt. Where nothing can ask, in the Python API or a command run
without a terminal, and you have not decided yet, sessions are uploaded by default: the tool
says so once per process (a Python warning, or on stderr), naming your owner ID and the
opt-outs, and makes your identity if there is none. Deciding with `brisk consent --accept`
or `--revoke` (`briskapi.consent(accept=True)` or `revoke=True`) replaces the default. Partial replays (`--limit-frames`) and live sessions you close before the end
always stay local (a broker session closed after at least 100 frames still uploads its
timing summary).

Opting out:

- decline at the first prompt (`n`);
- `brisk consent --revoke` (or `briskapi.consent(revoke=True)`) turns uploads off until you
  turn them back on with `brisk consent --accept`;
- `BRISK_UPLOAD=0` disables uploads for any process with that environment;
- `--no-upload` (`upload=False` in Python) keeps one run entirely local;
- `--no-upload-market-data` (`upload_market_data=False` on `brisk.connect()`) keeps one broker
  capture's market recording local while its timing summary is uploaded.

## What an upload contains

Each stored recording consists of four Parquet tables holding the decoded recording, a list
of where they are stored and `manifest.json`. A demo recording's market tables are the same
for everyone, so the archive keeps them once and each demo upload sends and stores only its
own timing table; downloading it returns the whole recording.

- **Market data you recorded**: the BRiSK demo's securities master and decoded auction
  quotes, or a broker session's (SBI or e-shiten) decoded securities
  master and auction quotes (prices, quantities, turnover, issue status, codes, flags and
  source clocks; on each side of the order book the ten best prices holding orders, with
  their quantities and order counts, the same for the closing-condition orders, and the
  whole board's order totals); and every trade, with its time, price, shares and side,
  including those the session's snapshot restores from earlier that day. Version 5 added
  the order book, turnover and broker issue status; version 6 the trades, the
  closing-condition books and the board totals. Demo content is checked against the pinned
  reference replay. Broker content is uploader-declared, with exact field and continuity
  checks rather than a reference replay.
- **Local timing measurements**: per-frame decode time (`decode_ns`), your computer's
  wall-clock receipt time in milliseconds (`received_unix_ms`), replay lateness, total
  replay time and demo asset download time, plus whether the assets came from the website
  or a local cache. The receipt clock shows the date and time you made the recording.
  Timing values can loosely reflect your computer's speed and load.
- **Manifest**: a summary (source, trading date, securities covered, clock range, counts,
  sizes and SHA-256).

Broker packages include per-frame receipt/decode timing and session duration. They retain
only the site's fixed WebSocket origin as transport provenance (`sbi.brisk.jp` or
`next.e-shiten.brisk.jp`); connection and decoder diagnostics are removed before packaging.

## What is never collected

The tool sends nothing about brokerage or BRiSK accounts, cookies, session tokens,
passkeys, orders, usernames, hostnames, file paths, environment variables or hardware
identifiers. Your identity secret travels only to the archive's function, over HTTPS, to
prove which owner ID a request is for; the service keeps the owner ID, never the secret. The
archive service accepts only the fields listed above and rejects any recording that
contains other fields.

## Broker BRiSK (SBI, e-shiten)

The BRiSK site client (`briskapi.brisk`; `brisk live --sbi` for SBI, `site="e-shiten"` and
`brisk live --site e-shiten` for Tachibana e-shiten BRiSK Next) uses session cookies
from your own browser. They are credentials:

- They are sent only to the site that issued them, `https://sbi.brisk.jp` or
  `https://next.e-shiten.brisk.jp`, together with the API token it issues. The
  client never follows redirects, so they can't be forwarded elsewhere. The live
  stream may be on another BRiSK host (`api.brisk.jp`); like a browser, the client
  sends it the session named in the site's boot response, not your cookies, and
  refuses a stream outside `brisk.jp`.
- They stay in memory unless you call `brisk.login(..., remember=True)`, which saves
  them to `~/.config/brisk/sbi-cookies.json` (`e-shiten-cookies.json` for e-shiten;
  or `$XDG_CONFIG_HOME/brisk/`), readable only by you. `brisk.logout()` deletes both.
- The live host receives them through its environment, not its command line.
- `brisk enroll` / `brisk login` (passkey sign-in to the broker you choose: SBI, Matsui, Monex,
  SMBC Nikko, Tachibana e-shiten or one you add) capture a passkey that you register at that broker and use it to
  obtain BRiSK session cookies. The passkey's private key is a credential that lets whoever
  has it sign in to your brokerage account. It is kept, together with the name of the broker (and, for
  a broker whose passkey form asks for it first, such as e-shiten, your user ID), only
  in the macOS Keychain or Windows Credential Manager (or, if you set `BRISK_PASSKEY_STORE=file`, in
  an owner-only file under `~/.config/brisk/`, which on Windows is protected by your user profile
  folder), is passed to the sign-in helper over stdin and never on a command line,
  and is never logged, uploaded or included in any recording. `brisk forget` deletes it here;
  remove it at the broker as well. The user ID is typed into the broker's login page and is never
  logged either. SBI's and e-shiten's cookies are saved as described above; those of a broker
  without a data client are kept in an owner-only file under `~/.config/brisk/cookies/`. The Chrome profile used for sign-in is
  temporary and deleted afterwards (one left by a force-killed run is removed at the next start),
  and only the BRiSK host's cookies are taken from it.
- With uploads on, each trading day of a broker session's decoded market recording (SBI or
  e-shiten) is uploaded after that day ends. A day still open when you stop the session,
  `--no-upload-market-data` or `brisk.connect(upload_market_data=False)` keeps it local.
- With uploads on, a broker session (SBI or e-shiten) uploads one **timing summary** per
  trading day to your private archive when the day ends (or when you close it after at least
  100 frames): the
  p50/p90/p99/max of per-frame decode time, of the feed clock's age at local receipt and of
  the spacing between frames; the number of gaps over one second; the frame count; the
  trading date; the first and last minute (JST) of the session; and the briskapi version.
  `upload=False` on `brisk.connect()` or `BRISK_UPLOAD=0` keeps both the summary and any market
  recording local. The environment opt-out suppresses the interactive question as well.

## Network metadata

Uploading, listing, downloading and deleting contact an AWS Lambda function URL and Amazon S3
in Tokyo (`ap-northeast-1`). These services receive your IP address with each request.

- The service uses your IP address only to rate limit upload tickets (four per hour). It
  stores a keyed hash (HMAC-SHA-256 with a secret per-deployment key), never the address
  itself, and your owner ID for a per-owner limit. These entries are marked to expire after
  two hours, and DynamoDB deletes expired entries, usually within a few days.
- The service does not log IP addresses, identity secrets or recording contents. Its logs
  record infrastructure errors and are deleted after 14 days. S3 server access logging is
  not enabled.

`--web` recording downloads the demo's assets from `https://next-demo.brisk.jp/`. That
request goes to BRiSK's servers and is covered by their policies, not this one.

## Storage, access and retention

- **Stored recordings and timing reports are private to their owner ID** and kept until you
  delete them (or the operator removes them). What you delete stays restorable for 30 days
  and is removed by the storage within a day after that.
  Recordings are content addressed under your owner ID: the same recording uploaded twice is
  stored once. Their names carry the trading date and their first and last clock time.
- Uploads are validated in a private staging area and deleted immediately after validation,
  whether they are accepted or rejected. Ticket metadata (manifest, owner ID and status) in
  staging expires after two days. Rejected uploads are never stored.
- Recordings stored before policy version 6, when the archive was public, are no longer
  public or reachable through the tool; the operator holds them and removes them on request.
  The exception is the legacy wire-format sample capture (2026-03-11), copied to `samples/`
  as a public download.
- On your computer, the tool stores your upload choice in `~/.config/brisk/uploads.json` and
  your identity in `~/.config/brisk/identity.json` (or `$XDG_CONFIG_HOME/brisk/`) and in the
  system's credential store under `briskapi-archive-identity`.
  `briskapi.pull()` caches verified downloads in `~/.cache/brisk/` (or
  `$XDG_CACHE_HOME/brisk/`). Delete these files at any time; deleting every copy of the
  identity without its recovery code ends access to what it uploaded.

## Operator and changes

The archive is operated by the owner of
[honvl/briskapi](https://github.com/honvl/briskapi). AWS provides the
infrastructure. Send removal requests and privacy questions to the operator through
that GitHub account. Do not post personal data in a public issue.

If a future version collects anything beyond what is described here, or uses it otherwise,
the policy version increases. Earlier choices then no longer apply: the command line asks
again before uploading, and where nothing can ask, the tool says so once, as it does before
you first decide.
