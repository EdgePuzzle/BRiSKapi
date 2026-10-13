"""Python API for BRiSK auction data: recordings, your private archive and the public demo.

    import briskapi

    feed = briskapi.connect(web=True)          # live: the demo replay (site="sbi" or "e-shiten": your broker's BRiSK)
    briskapi.Ticker("7203").quote()            # current auction quote
    for q in feed.quotes("7203"): ...          # or feed.on_quote(callback)

    briskapi.pull(prefix)                      # or briskapi.load(path) / briskapi.record(...)
    briskapi.Ticker("7203").quote(at="08:59:59")
    briskapi.Market().imbalances(top=10).to_pandas()

    live = briskapi.Live()                     # Databento-style records from `brisk live` on this computer
    live.subscribe(dataset="XTKS.BRISK", schema="imbalance", symbols=["7203"])
    for record in live: ...
    briskapi.Historical().timeseries.get_range(...)   # the same records from your archive in the cloud
"""
from __future__ import annotations

from briskapi import brisk as _brisk, cli as _cli
from briskapi._archive import Archive
from briskapi._gateway import Live, Record
from briskapi._historical import Historical
from briskapi._live import Feed, connect as _connect, record as _record, stream
from briskapi._market import Market, Ticker
from briskapi._recording import JST, BriskError, NotFoundError, Recording, Table

__version__ = '1.0.1'
__all__ = ['JST', 'Archive', 'BriskError', 'Feed', 'Historical', 'Live', 'Market', 'NotFoundError', 'Record', 'Recording',
           'Table', 'Ticker', 'connect', 'consent', 'current', 'load', 'login', 'logout', 'pull', 'record', 'recordings',
           'stream']

_current: Recording | Feed | None = None


def load(source) -> Recording | Feed:
    """Use a live Feed or a recording (path, directory or Recording) as the default for Ticker and Market."""
    global _current
    _current = source if isinstance(source, (Recording, Feed)) else Recording(source)
    return _current


def current() -> Recording | Feed:
    if _current is None:
        raise BriskError('Nothing loaded: use briskapi.connect(...), briskapi.load(path), briskapi.pull(prefix) or briskapi.record(...)')
    return _current


_DEMO_ONLY = {'speed': 1, 'limit_frames': None}
_BROKER_ONLY = {'trace_protocol': False, 'profile': None, 'upload_market_data': None}


def connect(*, web=False, cache=None, site=None, codes=None, history=False, upload=None, output=None, on_day=None,
            gateway=False, timeout=None, node='node', speed=1, limit_frames=None, trace_protocol=False, profile=None,
            upload_market_data=None) -> Feed:
    """Start a live feed and make it the default for Ticker and Market, as `brisk live` does. One source:

    - web=True: the public BRiSK Next demo, a recording of 27 September 2021 that BRiSK's demo site
      serves as files; they are downloaded and replayed at their recorded pace (`speed`, `limit_frames`).
    - cache=DIR: the same demo from a copy downloaded before.
    - site="sbi" or "e-shiten": your broker's live BRiSK (experimental), signing in as login(site) does
      unless login already did (`trace_protocol`, `profile`, `upload_market_data`).

    The feed records each trading day and uploads it to your private archive unless you opt out
    (upload=False); `output` also keeps each day on disk, `on_day(result)` is called with each,
    `history=True` keeps every update for Ticker.history(), and gateway=True also serves the feed to
    briskapi.Live on this computer. `timeout` (default 60 s, 120 s for a broker) bounds the wait for the
    initial state.
    """
    if sum(map(bool, (web, cache, site))) != 1:
        raise ValueError('Choose one source: web=True or cache=DIR (the demo), or site="sbi" or "e-shiten" (your broker)')
    common = dict(codes=codes, history=history, upload=upload, output=output, on_day=on_day, gateway=gateway, node=node)
    given = dict(speed=speed, limit_frames=limit_frames, trace_protocol=trace_protocol, profile=profile,
                 upload_market_data=upload_market_data)
    wrong = [name for name in (_DEMO_ONLY if site else _BROKER_ONLY) if given[name] != (_DEMO_ONLY | _BROKER_ONLY)[name]]
    if wrong:
        raise ValueError(f"{', '.join(wrong)}: only for {'the demo' if site else 'a broker site'}")
    if site:
        return load(_brisk._connect(site, timeout=timeout or 120, trace_protocol=trace_protocol, profile=profile,
                                    upload_market_data=upload_market_data, **common))
    return load(_connect(web=web, cache=cache, speed=speed, limit_frames=limit_frames, timeout=timeout or 60, **common))


def login(site='sbi', *, cookies=None, remember=False) -> _brisk.Client:
    """Sign in to your broker's BRiSK, site "sbi" (the default) or "e-shiten", with its session cookies: given
    (copied from your logged-in browser), from the site's variable (BRISK_SBI_COOKIES, BRISK_ESHITEN_COOKIES;
    JSON) or saved by remember=True or `brisk login` (passkey). Ticker's and Market's broker data
    (candles, margin, turnover, lists, events, schedule, watchlist) then come from it, and
    connect(site=...) uses it."""
    return _brisk._login(site, cookies, remember)


def logout():
    """Forget the broker session, and the cookies saved for every site."""
    _brisk._logout()


def recordings(date=None, source=None) -> Table:
    """Your archived recordings (see Archive.recordings)."""
    return Archive().recordings(date, source)


def pull(prefix, output=None) -> Recording:
    """Download and verify an archive recording, then make it the default."""
    return load(Archive().pull(prefix, output))


def record(output, **options) -> Recording:
    """Record the public demo (see briskapi._live.record), upload it per consent and make it the default."""
    return load(_record(output, **options))


def consent(accept=False, revoke=False) -> dict | None:
    """Show (no arguments), accept or revoke automatic uploads to your private archive (on by default:
    `default: True` until you decide). Accepting makes your archive identity if there is none and
    prints it to stderr. See PRIVACY.md."""
    if accept and revoke:
        raise ValueError('Choose accept or revoke')
    if accept:
        return _cli.accept()
    return _cli.save_consent(False) if revoke else _cli.load_consent() or dict(_cli.DEFAULT)
