"""The BRiSK site client (SBI, e-shiten) with pybrisk's SBI sample payloads and a fake HTTP layer (no account needed)."""
import base64
import datetime as dt
import io
import json
import os
from pathlib import Path
import stat
import sys
import time
import urllib.error
import zlib

from briskapi.timing import TimingStats

import pytest

import briskapi
import briskapi.cli as cli
import briskapi.schema as schema
from briskapi import _parquet, _spool, brisk
from briskapi._recording import JST, NotFoundError

CHECK_SESSION = brisk._check_session  # stubbed for every test below; the session tests put it back

BOOT = {'result': True, 'user_id': 'test-uuid', 'api_token': 'v2.local.testtoken', 'api_endpoint': 'https://api.brisk.jp'}
APP_BOOT = {'result': True, 'series': 0, 'date': '2026-03-11', 'session_status': 'running',
            'ws_url': '/realtime/0?session=abc', 'master': 'masterhash', 'snapshot': 'snaphash',
            'schedule_info': {'morning_session_pre_open_time': 28800000000, 'morning_session_open_time': 32400000000,
                              'morning_session_close_time': 41400000000, 'afternoon_session_pre_open_time': 43500000000,
                              'afternoon_session_open_time': 45000000000, 'afternoon_session_pre_close_time': 55500000000,
                              'afternoon_session_close_time': 55800000000, 'sq_jump_interval': 180}}
OHLC = {'ohlc5min': [{'date': '2026-03-11', 'index': 0, 'diff': 0, 'open_price': 2000, 'high_price': 2050,
                      'low_price': 1980, 'close_price': 2030, 'turnover': 100000000}],
        'ohlc1day': [{'date': '2026-03-11', 'open_price': 2000, 'high_price': 2100, 'low_price': 1950,
                      'close_price': 2080, 'turnover': 500000000}],
        'ohlc1week': [{'year': 2026, 'week': 10, 'open_price': 1, 'high_price': 2, 'low_price': 1, 'close_price': 2, 'turnover': 3}],
        'ohlc1month': [{'year': 2026, 'month': 3, 'open_price': 1, 'high_price': 2, 'low_price': 1, 'close_price': 2, 'turnover': 3}]}
JSFC = {'0': {'date': '2026-03-11', 'kakuhoLongShares': 100000, 'kakuhoShortShares': 200000, 'gyakuhibuFee': 0.05}}
MARKETS = {'market_conditions': [{'index': 0, 'issue_code': '3655', 'kind': 1, 'type': 6, 'price10': 26910,
                                  'value10': 2691000000, 'diff_bps_from_last': 0, 'time': '08:00:00.048598'}]}


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass


class Opener:
    def __init__(self, routes):
        self.routes, self.requests = routes, []

    def open(self, request, timeout):
        self.requests.append(request)
        path = request.full_url.removeprefix(brisk.ORIGIN).split('?')[0]
        value = self.routes.get(path, 404)
        if isinstance(value, int):
            raise urllib.error.HTTPError(request.full_url, value, 'error', {}, io.BytesIO(b'details'))
        return Response(value if isinstance(value, bytes) else json.dumps(value).encode())


def client(**routes):
    opener = Opener({'/api/frontend/boot': BOOT, '/api/app/boot': APP_BOOT, **routes})
    session = brisk.Session({'session_bfaf77a2': 'v2.local.cookie'}, rate_limit=0, opener=opener)
    return brisk.Client(session=session), opener


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.delenv('BRISK_SBI_COOKIES', raising=False)
    monkeypatch.delenv('BRISK_ESHITEN_COOKIES', raising=False)
    monkeypatch.delenv('BRISK_UPLOAD', raising=False)
    monkeypatch.setattr(brisk, '_client', None)
    monkeypatch.setattr(briskapi, '_current', None)
    monkeypatch.setattr(brisk, '_check_session', lambda session: None)  # fake hosts below have no HTTP site behind them


def test_boot_and_headers():
    c, opener = client()
    assert c.date == '2026-03-11' and c.boot['series'] == 0
    boot, app = opener.requests
    assert boot.get_header('Cookie') == 'session_bfaf77a2=v2.local.cookie' and not boot.has_header('Authorization')
    assert app.get_header('Authorization') == 'Bearer v2.local.testtoken'
    c.boot  # cached: no further requests
    assert len(opener.requests) == 2


def test_ticker_candles_and_margin():
    c, opener = client(**{'/api/ohlc/7203': OHLC, '/api/jsfc/7203': JSFC})
    t = briskapi.Ticker('7203', client=c)
    five = t.candles('5m')
    assert five == [{'date': '2026-03-11', 'bar': 0, 'open': 2000, 'high': 2050, 'low': 1980, 'close': 2030,
                     'turnover': 100000000}]
    assert t.candles()[0]['close'] == 2080 and t.candles('1w')[0]['week'] == 10 and t.candles('1mo')[0]['month'] == 3
    assert 'date=2026-03-11' in opener.requests[2].full_url
    with pytest.raises(ValueError, match='interval'):
        t.candles('1h')
    margin = t.margin(days=30)
    assert margin[0]['long_balance'] == 100000 and margin[0]['lending_fee'] == 0.05 and margin[0]['lending_fee_max'] is None
    assert 'count=30' in opener.requests[-1].full_url


def test_market_endpoints():
    groups = base64.b64encode(zlib.compress(json.dumps({'groups': [{'items': [{'code': '7203'}, {'x': 1}]}]}).encode()))
    flat = base64.b64encode(zlib.compress(json.dumps(['6758', {'code': '9984'}, {'x': 1}]).encode()))
    c, opener = client(**{
        '/api/stocks_info': [{'issue_code': '7203', 'turnover': 5000000000, 'calc_shares_outstanding': 1000000000}],
        '/api/stock_lists': {'version': '1', 'stock_lists': [{'id': 'nk225etf', 'name': 'NK225', 'issue_codes': ['1332', '7203']}]},
        '/api/markets': MARKETS,
        '/api/frontend/watchlist': {'empty': False, 'data': groups.decode()}})
    m = briskapi.Market(client=c)
    assert m.turnover() == [{'code': '7203', 'turnover': 5000000000, 'shares_outstanding': 1000000000}]
    assert m.lists() == {'nk225etf': ['1332', '7203']}
    event = m.events()[0]
    assert event['price'] == 2691.0 and event['value'] == 269100000.0 and event['code'] == '3655'
    assert event['change_bps'] == 0 and event['time'] == dt.datetime(2026, 3, 11, 8, 0, 0, 48598, tzinfo=JST)
    assert 'series=0' in opener.requests[-1].full_url and 'index_to=618' in opener.requests[-1].full_url
    schedule = m.schedule()
    assert schedule['status'] == 'running' and schedule['morning_open'] == dt.datetime(2026, 3, 11, 9, tzinfo=JST)
    assert schedule['afternoon_close'].hour == 15 and schedule['afternoon_close'].minute == 30
    assert m.watchlist() == ['7203']
    opener.routes['/api/frontend/watchlist'] = {'empty': False, 'data': flat.decode()}
    assert m.watchlist() == ['6758', '9984']
    opener.routes['/api/frontend/watchlist'] = {'empty': True}
    assert m.watchlist() == []


@pytest.mark.parametrize('status,error', [(401, brisk.SessionExpiredError), (302, brisk.SessionExpiredError),
                                          (404, briskapi.NotFoundError), (429, brisk.RateLimitError), (500, brisk.APIError)])
def test_errors(status, error):
    c, _ = client(**{'/api/ohlc/7203': status})
    with pytest.raises(error):
        c.candles('7203')
    assert str(brisk.APIError(500)) == 'HTTP 500'


def test_session_rules(monkeypatch):
    for cookies in ({}, None, {'session_bfaf77a2': ''}, []):
        with pytest.raises(brisk.SessionExpiredError, match='login'):
            brisk.Session(cookies)
    assert brisk._NoRedirect().redirect_request(None, None, 302, 'Found', {}, 'https://elsewhere.test/') is None
    slept = []
    monkeypatch.setattr(brisk.time, 'sleep', slept.append)
    session = brisk.Session({'a': 'b'}, rate_limit=2, opener=Opener({'/raw': b'\x00\x01'}))
    assert session.get('/raw', raw=True) == b'\x00\x01'
    session.get('/raw', raw=True)
    assert slept and 0 < slept[0] <= 0.5


def test_login_sources(monkeypatch, tmp_path):
    with pytest.raises(brisk.SessionExpiredError, match='needs a session'):
        briskapi.Ticker('7203').candles()
    with pytest.raises(brisk.SessionExpiredError):
        briskapi.login()
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"from": "env"}')
    assert briskapi.login().session.cookies == {'from': 'env'}
    monkeypatch.delenv('BRISK_SBI_COOKIES')
    briskapi.login(cookies={'saved': 'yes'}, remember=True)
    path = brisk.cookies_path()
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600 and json.loads(path.read_text()) == {'saved': 'yes'}
    assert briskapi.login().session.cookies == {'saved': 'yes'}
    brisk._client.session._opener = Opener({'/api/frontend/boot': BOOT, '/api/app/boot': APP_BOOT})
    brisk._client.session.rate_limit = 0
    assert briskapi.Market().schedule()['date'] == '2026-03-11'  # default session used
    briskapi.logout()
    assert not path.exists() and brisk._client is None


OPEN_US = 9 * 3600 * 1_000_000
MIDNIGHT_MS = int(dt.datetime(2026, 3, 11, tzinfo=JST).timestamp() * 1000)


def sbi_session(frames=150):
    """Bootstrap, `frames` one-quote batches 100 ms apart (20 ms old on receipt, one 1.5 s stall) and end."""
    q = {**dict.fromkeys(schema.QUOTE_KEYS, 0), **{key: [] for key in schema.BOOK_KEYS},
         'issue_id': 0, 'code': '7203', 'frame': 1, 'max_frame': 1, 'source_time_us': OPEN_US,
         'indicative_price10': 101500, 'market_buy_quantity': 3, 'market_sell_quantity': 1}
    master = {**dict.fromkeys(schema.MASTER_KEYS, 0), 'issue_id': 0, 'code': '7203', 'name': 'Toyota', 'lot_size': 100}
    batches = [dict(type='bootstrap', seq=0, source='sbi_live', trading_date='20260311', source_time_us=OPEN_US,
                    input_transport={**schema.SBI_TRANSPORT, 'decoder': '/private/decoder.js', 'setup_ms': 1.123456},
                    source_timestamp_origin='brisk_decoder_unverified', exchange_delay_ms=None,
                    market_issue_count=1, master=[master], quotes=[q], trades=[])]
    for i in range(1, frames + 1):
        now = OPEN_US + i * 100_000 + (1_400_000 if i > 100 else 0)
        batches.append(dict(type='quotes', seq=i, source_time_us=now, received_unix_ms=MIDNIGHT_MS + now // 1000 + 20,
                            decode_ns=250_000 + i * 1000, replay_lateness_ms=None,
                            quotes=[{**q, 'frame': i + 1, 'max_frame': i + 1, 'source_time_us': now,
                                     'last_price10': 101500 + 100 * (i == frames)}], trades=[]))
    return batches + [dict(type='end', seq=frames + 1, source_time_us=now, frames=frames, quote_updates=frames,
                          replay_wall_ms=16_400)]


@pytest.fixture
def fake_host(tmp_path, monkeypatch):
    """A stand-in for brisk.cjs that checks its cookies and prints a short live session."""
    batches = sbi_session()
    script = tmp_path / 'sbi_fake.cjs'
    script.write_text(
        "const c = JSON.parse(process.env.BRISK_SBI_COOKIES); if (c.session_bfaf77a2 !== 'v') process.exit(9);\n"
        f"console.error(JSON.stringify(process.argv.slice(2)));\n"
        f"for (const b of {json.dumps(batches)}) console.log(JSON.stringify(b));\n")
    monkeypatch.setattr(brisk, 'DECODER', script)


def test_live_feed_via_node(fake_host, capfd):
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    feed = briskapi.connect(site='sbi', codes=['7203'], history=True, upload=False)
    assert briskapi.current() is feed and feed.source == 'sbi_live'
    feed.wait()
    assert briskapi.Ticker('7203').quote()['last_price'] == 10160.0 and feed.uploaded is None
    assert len(briskapi.Ticker('7203').history()) == 151 and feed.timing_uploaded is None
    # Cookies reach the host through its environment, never its (world-readable) arguments.
    assert capfd.readouterr().err.strip() == '["--codes","7203"]'


def test_connect_signs_in_from_the_sites_variable_when_login_has_not(fake_host, monkeypatch):
    briskapi.logout()
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    feed = briskapi.connect(site='sbi', upload=False).wait()
    assert feed.source == 'sbi_live' and brisk._default().site == 'sbi'


def test_connect_takes_a_site_and_only_its_options(tmp_path):
    for site in (None, 'web', 'matsui'):
        with pytest.raises(ValueError, match="Choose a site: 'demo', 'sbi', 'e-shiten'"):
            briskapi.connect(site=site)
    with pytest.raises(ValueError, match='cache, speed: only for the demo'):
        briskapi.connect(site='sbi', cache=tmp_path, speed=0)
    with pytest.raises(ValueError, match='trace_protocol, upload_market_data: only for a broker site'):
        briskapi.connect(site='demo', cache=tmp_path, trace_protocol=True, upload_market_data=False)
    with pytest.raises(TypeError, match='cookies by name'):
        briskapi.login({'session_bfaf77a2': 'v'})


def test_protocol_options_reach_the_host_without_putting_the_profile_in_argv(tmp_path, monkeypatch, capfd):
    script = tmp_path / 'sbi_options.cjs'
    script.write_text(
        "console.error(JSON.stringify({argv: process.argv.slice(2), profile: process.env.BRISK_PROFILE || null}));\n"
        f"for (const b of {json.dumps(sbi_session())}) console.log(JSON.stringify(b));\n")
    monkeypatch.setattr(brisk, 'DECODER', script)
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    profile = {'connectQuery': {'_v': 'build-9'}, 'catchUp': 'issue-lists'}
    briskapi.connect(site='sbi', codes=['7203'], trace_protocol=True, profile=profile, upload=False).wait()
    seen = json.loads(capfd.readouterr().err.strip())
    assert seen['argv'] == ['--codes', '7203', '--trace-protocol']
    assert json.loads(seen['profile']) == profile
    assert 'build-9' not in ' '.join(seen['argv'])
    briskapi.connect(site='sbi', codes=['7203'], upload=False).wait()
    assert json.loads(capfd.readouterr().err.strip()) == {'argv': ['--codes', '7203'], 'profile': None}


def test_live_feed_failure_closes(fake_host, tmp_path, monkeypatch):
    briskapi.login(cookies={'session_bfaf77a2': 'wrong'})
    with pytest.raises(briskapi.BriskError, match='before bootstrap'):
        briskapi.connect(site='sbi')


def test_cli_live_sbi(fake_host, monkeypatch, capsys):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO('\n'))
    sent, market = [], []
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: sent.append(report) or {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload', lambda *args, **kwargs: market.append(args) or {'status': 'stored'})
    cli.main(['live', '--site', 'sbi', '--codes', '7203'])
    out = capsys.readouterr()
    lines = [json.loads(line) for line in out.out.splitlines()]
    # The one-time question names what a broker session uploads: a timing summary and the market recording.
    assert lines[-1]['last_price'] == 10160.0 and 'timing summary' in out.err and 'decoded market recording' in out.err
    assert cli.load_consent()['enabled'] and sent[0]['source'] == 'sbi_live' and len(market) == 1
    assert not {'quotes', 'master', 'code', 'price'} & set(json.dumps(sent[0]).replace('"', ' ').split())


def test_cli_trace_protocol_reaches_the_sbi_host(fake_host, monkeypatch, capfd):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    cli.main(['live', '--site', 'sbi', '--codes', '7203', '--trace-protocol', '--no-upload'])
    assert capfd.readouterr().err.count('["--codes","7203","--trace-protocol"]') == 1


def test_sbi_feed_uploads_timing(fake_host, monkeypatch):
    sent = []
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: sent.append((report, url)) or {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload', lambda *args, **kwargs: {'status': 'stored'})
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    with pytest.warns(UserWarning, match='by default'):
        feed = briskapi.connect(site='sbi', upload_market_data=False).wait()
    assert feed.timing_uploaded == {'status': 'stored'} and feed.uploaded is None
    report, url = sent[0]
    assert url == cli.settings()['api_url'] and 'contributor' not in report
    assert report['frames'] == 150 and report['stalls'] == 1 and report['trading_date'] == '20260311'
    assert report['first_minute'] == '09:00' and report['source_age_ms']['p50'] == 20
    assert report['interarrival_ms']['max'] == 1500 and report['decode_ms']['p50'] > 0.25
    assert briskapi.connect(site='sbi', upload=False).wait().timing_uploaded is None and len(sent) == 1
    monkeypatch.setattr(cli, 'upload_timing', lambda *a: 1 / 0)
    with pytest.warns(UserWarning, match='Timing upload failed'):
        assert briskapi.connect(site='sbi').wait().timing_uploaded['status'] == 'failed'


def test_cli_without_cookies_stops_before_prompt_or_decoder(monkeypatch, capsys):
    monkeypatch.setattr(cli, 'check_node', lambda *args: pytest.fail('decoder must not start'))
    monkeypatch.setattr(cli, 'ask_consent', lambda: pytest.fail('do not prompt without cookies'))
    with pytest.raises(SystemExit, match='No SBI BRiSK cookies'):
        cli.main(['live', '--site', 'sbi'])
    assert 'Decoder exited' not in capsys.readouterr().err


def test_an_expired_session_fails_in_one_line_before_the_decoder_starts(monkeypatch, capsys):
    import briskapi._live
    monkeypatch.setattr(brisk, '_check_session', CHECK_SESSION)
    monkeypatch.setattr(briskapi._live, 'Feed', lambda *a, **k: pytest.fail('decoder must not start'))
    opener = Opener({'/api/frontend/boot': 401})
    monkeypatch.setattr(brisk.urllib.request, 'build_opener', lambda *handlers: opener)
    briskapi.login(cookies={'session_bfaf77a2': 'expired'})
    with pytest.raises(brisk.SessionExpiredError, match='^SBI BRiSK session expired or invalid; log in again$'):
        briskapi.connect(site='sbi', codes=['7203'])
    assert [r.full_url for r in opener.requests] == [brisk.ORIGIN + '/api/frontend/boot']
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "expired"}')
    with pytest.raises(SystemExit) as stop:
        cli.main(['live', '--site', 'sbi', '--codes', '7203'])
    assert stop.value.code == 'brisk: error: SBI BRiSK session expired or invalid; log in again'
    assert 'Decoder exited' not in capsys.readouterr().err


def test_cookies_that_are_not_json_fail_in_one_line_without_their_value(monkeypatch):
    monkeypatch.setenv('BRISK_ESHITEN_COOKIES', '{session_bfaf77a2: secret-value}')
    with pytest.raises(brisk.SessionExpiredError, match='^BRISK_ESHITEN_COOKIES is not valid JSON') as error:
        briskapi.login(site='e-shiten')
    assert 'secret-value' not in str(error.value) and 'line 1 column 2' in str(error.value)
    with pytest.raises(SystemExit) as stop:
        cli.main(['live', '--site', 'e-shiten'])
    assert stop.value.code.startswith('brisk: error: BRISK_ESHITEN_COOKIES is not valid JSON') and '\n' not in stop.value.code
    monkeypatch.delenv('BRISK_ESHITEN_COOKIES')
    brisk.cookies_path().parent.mkdir(parents=True, exist_ok=True)
    brisk.cookies_path().write_text('session_bfaf77a2=secret-value')
    with pytest.raises(brisk.SessionExpiredError, match='is not valid JSON') as error:
        briskapi.login()
    assert str(brisk.cookies_path()) in str(error.value) and 'secret-value' not in str(error.value)


def test_sbi_market_upload_needs_uploads_on(fake_host, monkeypatch):
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(revoke=True)
    with pytest.raises(briskapi.BriskError, match='uploads on'):
        briskapi.connect(site='sbi', upload_market_data=True)
    # A choice made under an earlier policy no longer applies: the default does, and says so.
    cli.consent_path().write_text(json.dumps({'policy_version': 2, 'enabled': False}))
    monkeypatch.setattr(cli, 'upload', lambda *args, **kwargs: {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload_timing', lambda *args: {'status': 'stored'})
    with pytest.warns(UserWarning, match='by default'):
        assert briskapi.connect(site='sbi', upload_market_data=True).wait().uploaded == {'status': 'stored'}


def test_sbi_market_upload_follows_consent_and_opt_outs(fake_host, monkeypatch):
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    sent, timing = [], []
    def publish(directory, url, **options):
        m = json.loads((directory / 'manifest.json').read_text())
        _parquet.inspect_package(directory, m)
        sent.append(m)
        return {'status': 'stored'}
    monkeypatch.setattr(cli, 'upload', publish)
    monkeypatch.setattr(cli, 'upload_timing', lambda *args: timing.append(args) or {'status': 'stored'})
    # Once agreed, a clean session uploads its market recording too, without asking.
    feed = briskapi.connect(site='sbi').wait()
    assert feed.uploaded == {'status': 'stored'} and len(sent) == 1 and len(timing) == 1
    assert sent[0]['summary']['source'] == 'sbi_live' and set(sent[0]) == {'schema', 'sha256', 'bytes', 'summary'}
    assert not any(_spool.spool_root().glob('segment-*')), 'the day leaves no spool behind'
    assert briskapi.connect(site='sbi', upload_market_data=True).wait().uploaded == {'status': 'stored'} and len(sent) == 2
    # The opt-outs: the market recording alone, the whole session, or the whole process.
    feed = briskapi.connect(site='sbi', upload_market_data=False).wait()
    assert feed.uploaded is None and feed.timing_uploaded == {'status': 'stored'} and len(sent) == 2
    feed = briskapi.connect(site='sbi', upload=False).wait()
    assert feed.uploaded is None and feed.timing_uploaded is None and len(timing) == 3
    with pytest.raises(briskapi.BriskError, match='upload=False keeps the whole session local'):
        briskapi.connect(site='sbi', upload_market_data=True, upload=False)
    monkeypatch.setenv('BRISK_UPLOAD', '0')
    feed = briskapi.connect(site='sbi').wait()
    assert feed.uploaded is None and feed.timing_uploaded is None and len(sent) == 2 and len(timing) == 3
    briskapi.consent(revoke=True)
    monkeypatch.delenv('BRISK_UPLOAD')
    assert briskapi.connect(site='sbi').wait().uploaded is None and len(sent) == 2 and len(timing) == 3


@pytest.mark.parametrize('answer,uploaded', [('\n', True), ('y\n', True), ('n\n', False), ('', False)])
def test_cli_asks_once_then_uploads_sbi_captures_automatically(fake_host, monkeypatch, capsys, answer, uploaded):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    sent, timing = [], []
    monkeypatch.setattr(cli, 'upload', lambda *args, **kwargs: sent.append(args) or {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload_timing', lambda *args: timing.append(args) or {'status': 'stored'})
    for run in range(2):
        monkeypatch.setattr(cli.sys, 'stdin', io.StringIO(answer))
        cli.main(['live', '--site', 'sbi'])
        out = capsys.readouterr()
        assert ('Upload automatically to your private archive? [Y/n]' in out.err) == (run == 0)
        assert len(sent) == len(timing) == (run + 1 if uploaded else 0)
    assert cli.load_consent()['enabled'] == uploaded
    cli.main(['live', '--site', 'sbi', '--no-upload'])
    assert len(sent) == len(timing) == (2 if uploaded else 0)


@pytest.mark.parametrize('flags,market,timing', [((), 1, 1), (('--upload-market-data',), 1, 1),
                                                   (('--no-upload-market-data',), 0, 1), (('--no-upload',), 0, 0)])
def test_cli_sbi_upload_switches(fake_host, monkeypatch, flags, market, timing):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    briskapi.consent(accept=True)
    monkeypatch.setattr(cli, 'ask_consent', lambda: pytest.fail('asked once already'))
    sent = {'market': [], 'timing': []}
    monkeypatch.setattr(cli, 'upload', lambda *args, **kwargs: sent['market'].append(args) or {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload_timing', lambda *args: sent['timing'].append(args) or {'status': 'stored'})
    cli.main(['live', '--site', 'sbi', *flags])
    assert (len(sent['market']), len(sent['timing'])) == (market, timing)


def test_a_feed_stopped_mid_day_ends_the_day_there_and_uploads_it(tmp_path, monkeypatch):
    # A host that, asked to stop (SIGTERM, as close() sends), ends its day with its end, as brisk.cjs does.
    batches = sbi_session(frames=150)
    first = batches[:41]
    end = dict(batches[-1], seq=41, source_time_us=first[-1]['source_time_us'], frames=40, quote_updates=40)
    script = tmp_path / 'stoppable.cjs'
    script.write_text(f"""for (const b of {json.dumps(first)}) console.log(JSON.stringify(b));
process.on('SIGTERM', () => {{ process.stdout.write(JSON.stringify({json.dumps(end)}) + '\\n', () => process.exit(0)); }});
setInterval(() => {{}}, 1000);
""")
    monkeypatch.setattr(brisk, 'DECODER', script)
    sent = []
    def publish(directory, url, **options):
        m = json.loads((directory / 'manifest.json').read_text())
        _parquet.inspect_package(directory, m)
        sent.append(m['summary'])
        return {'status': 'stored'}
    monkeypatch.setattr(cli, 'upload', publish)
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: {'status': 'stored'})
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    feed = briskapi.connect(site='sbi', codes=['7203'], output=tmp_path / 'days')
    deadline = time.monotonic() + 20
    while feed.seq < 40:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    feed.close()
    assert feed.status == 'closed' and feed.uploaded == {'status': 'stored'}
    assert [s['batches'] for s in sent] == [42] and sent[0]['quote_updates'] == 41   # the bootstrap's, then 40
    name, = [day['name'] for day in feed.recordings]
    assert name == 'sbi_live_20260311_090000-090004' and (tmp_path / 'days' / name / 'manifest.json').exists()
    assert not any(_spool.kept_dir().glob('*')) and not any(_spool.spool_root().glob('segment-*'))


def test_a_capture_whose_host_cannot_end_its_day_is_kept_but_not_uploaded(tmp_path, monkeypatch):
    script = tmp_path / 'held.cjs'
    script.write_text(f'console.log({json.dumps(json.dumps(sbi_session()[0]))});\nsetInterval(() => {{}}, 1000);\n')
    monkeypatch.setattr(brisk, 'DECODER', script)
    monkeypatch.setattr(cli, 'upload', lambda *args, **kwargs: pytest.fail('incomplete capture must stay local'))
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    feed = briskapi.connect(site='sbi', upload_market_data=True)
    with pytest.warns(UserWarning, match='stopped before the day ended'):
        feed.close()
    assert feed.status == 'closed' and feed.uploaded is None
    kept, = _spool.kept_dir().iterdir()
    assert kept.name.startswith('sbi_live_20260311_') and not (kept / 'manifest.json').exists()
    assert briskapi.Market(briskapi.Recording(kept)).summary()['trading_date'] == '20260311'
    assert not any(_spool.spool_root().glob('segment-*'))


def two_days(tmp_path, monkeypatch, second=None):
    """A stand-in host that records two trading days back to back (each bootstrap ... end)."""
    first = sbi_session()
    second = second or [{**b, 'trading_date': '20260312'} if b['type'] == 'bootstrap' else
                        ({**b, 'quotes': [{**b['quotes'][0], 'last_price10': 102000}]} if b['type'] == 'quotes' else b)
                        for b in sbi_session(frames=120)]
    script = tmp_path / 'two_days.cjs'
    script.write_text(f'for (const b of {json.dumps(first + second)}) console.log(JSON.stringify(b));\n')
    monkeypatch.setattr(brisk, 'DECODER', script)


def later(batches, us):
    """The same session, `us` later on the clock (a reconnect's segment on the same trading day)."""
    def shift(item):
        item = dict(item)
        for key in ('source_time_us', 'time_us'):
            if key in item:
                item[key] += us
        if 'received_unix_ms' in item:
            item['received_unix_ms'] += us // 1000
        for key in ('quotes', 'trades'):
            if key in item:
                item[key] = [shift(x) for x in item[key]]
        return item
    return [shift(b) for b in batches]


def test_a_reconnect_keeps_and_uploads_the_segment_before_it_as_its_own_recording(tmp_path, monkeypatch):
    # The decoder closes a segment whose connection was lost with its own end, then records on from a new
    # snapshot of the same day: two recordings of that day, each named for its clocks.
    two_days(tmp_path, monkeypatch, second=later(sbi_session(frames=20), 3600 * 10**6))
    sent = []
    monkeypatch.setattr(cli, 'upload', lambda directory, url, **o: sent.append(directory.name) or {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: {'status': 'stored'})
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    feed = briskapi.connect(site='sbi', codes=['7203'], output=tmp_path / 'days').wait()
    assert [day['name'] for day in feed.recordings] == ['sbi_live_20260311_090000-090016', 'sbi_live_20260311_100000-100002']
    assert len(sent) == 2 and all(day['uploaded'] == {'status': 'stored'} for day in feed.recordings)
    assert not any(_spool.spool_root().glob('segment-*'))


def test_a_feed_keeps_and_uploads_each_trading_day_as_it_ends(tmp_path, monkeypatch):
    two_days(tmp_path, monkeypatch)
    sent, timing, days = [], [], []
    def publish(directory, url, **options):
        m = json.loads((directory / 'manifest.json').read_text())
        _parquet.inspect_package(directory, m)
        sent.append(m['summary']['trading_date'])
        return {'status': 'stored'}
    monkeypatch.setattr(cli, 'upload', publish)
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: timing.append(report['trading_date']) or {'status': 'stored'})
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    feed = briskapi.connect(site='sbi', codes=['7203'], output=tmp_path / 'days', on_day=days.append).wait()
    # One recording per day, named for its first and last source clock, kept and uploaded.
    assert sent == timing == ['20260311', '20260312'] and days == feed.recordings
    names = [day['name'] for day in feed.recordings]
    assert names == ['sbi_live_20260311_090000-090016', 'sbi_live_20260312_090000-090013']
    assert sorted(p.name for p in (tmp_path / 'days').iterdir()) == names
    assert briskapi.Recording(tmp_path / 'days' / names[0]).manifest['summary']['trading_date'] == '20260311'
    assert all(day['uploaded'] == day['timing_uploaded'] == {'status': 'stored'} for day in feed.recordings)
    # The feed's state is the latest day's.
    assert feed.trading_date == '20260312' and briskapi.Ticker('7203').quote()['last_price'] == 10200.0
    # Without uploads, days can still be kept; a new day must follow an end.
    briskapi.consent(revoke=True)
    feed = briskapi.connect(site='sbi', output=tmp_path / 'kept').wait()
    assert [day['uploaded'] for day in feed.recordings] == [None, None] and len(list((tmp_path / 'kept').iterdir())) == 2
    two_days(tmp_path, monkeypatch, second=sbi_session()[:3] + sbi_session())
    with pytest.raises(briskapi.BriskError, match='sequence error'):
        briskapi.connect(site='sbi', upload=False).wait()


def test_e_shiten_has_its_own_address_cookies_and_variable(monkeypatch):
    with pytest.raises(brisk.SessionExpiredError, match="No e-shiten BRiSK Next cookies.*site='e-shiten'"):
        briskapi.login(site='e-shiten')
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"for": "sbi"}')
    with pytest.raises(brisk.SessionExpiredError):  # SBI's cookies never go to another site
        briskapi.login(site='e-shiten')
    monkeypatch.setenv('BRISK_ESHITEN_COOKIES', '{"from": "env"}')
    c = briskapi.login(site='e-shiten')
    assert c.site == 'e-shiten' and c.session.cookies == {'from': 'env'}
    opener = Opener({})
    c.session._opener, c.session.rate_limit = opener, 0
    with pytest.raises(NotFoundError):
        c.session.get('/api/frontend/boot')
    assert opener.requests[0].full_url == 'https://next.e-shiten.brisk.jp/api/frontend/boot'
    monkeypatch.delenv('BRISK_ESHITEN_COOKIES')
    briskapi.login(cookies={'saved': 'e'}, remember=True, site='e-shiten')
    briskapi.login(cookies={'saved': 's'}, remember=True)
    assert json.loads(brisk.cookies_path('e-shiten').read_text()) == {'saved': 'e'}
    assert json.loads(brisk.cookies_path().read_text()) == {'saved': 's'}
    briskapi.logout()
    assert not brisk.cookies_path('e-shiten').exists() and not brisk.cookies_path().exists()
    with pytest.raises(ValueError, match='Unknown BRiSK site'):
        briskapi.login(cookies={'a': 'b'}, site='matsui')


@pytest.fixture
def eshiten_host(tmp_path, monkeypatch):
    """A stand-in for brisk.cjs on e-shiten that reports the site it was asked for and checks that its
    cookies came in e-shiten's own variable."""
    batches = sbi_session()
    batches[0] = {**batches[0], 'source': 'eshiten_live', 'input_transport': {
        **schema.BROKER_TRANSPORT['eshiten_live'], 'ws_host': 'api.brisk.jp', 'decoder': '/private/decoder.js'}}
    script = tmp_path / 'eshiten_fake.cjs'
    script.write_text('if (JSON.parse(process.env.BRISK_ESHITEN_COOKIES).s !== "v") process.exit(9);\n'
                      'console.error(process.env.BRISK_SITE);\n'
                      f'for (const b of {json.dumps(batches)}) console.log(JSON.stringify(b));\n')
    monkeypatch.setattr(brisk, 'DECODER', script)
    sent = {'timing': [], 'market': []}
    def publish(directory, url, **options):
        m = json.loads((directory / 'manifest.json').read_text())
        _parquet.inspect_package(directory, m)
        sent['market'].append(m)
        return {'status': 'stored'}
    monkeypatch.setattr(cli, 'upload', publish)
    monkeypatch.setattr(cli, 'upload_timing',
                        lambda report, url: sent['timing'].append(schema.validate_timing(report)) or {'status': 'stored'})
    return sent


def test_e_shiten_uploads_like_sbi_and_honors_opt_out(eshiten_host, monkeypatch, capfd):
    briskapi.login(cookies={'s': 'v'}, site='e-shiten')
    with pytest.warns(UserWarning, match='by default'):
        feed = briskapi.connect(site='e-shiten', codes=['7203']).wait()
    assert feed.source == 'eshiten_live' and briskapi.Ticker('7203').quote()['last_price'] == 10160.0
    assert feed.timing_uploaded == {'status': 'stored'} and feed.uploaded == {'status': 'stored'}
    assert eshiten_host['timing'][0]['source'] == 'eshiten_live'
    assert eshiten_host['market'][0]['summary']['source'] == 'eshiten_live'
    assert 'e-shiten' in capfd.readouterr().err
    assert briskapi.connect(site='e-shiten', upload_market_data=False).wait().uploaded is None
    assert briskapi.connect(site='e-shiten', upload=False).wait().timing_uploaded is None
    monkeypatch.setenv('BRISK_UPLOAD', '0')
    feed = briskapi.connect(site='e-shiten').wait()
    assert feed.uploaded is None and feed.timing_uploaded is None
    assert len(eshiten_host['market']) == 1 and len(eshiten_host['timing']) == 2


@pytest.mark.parametrize('answer,uploaded', [('\n', True), ('n\n', False)])
def test_cli_asks_once_for_e_shiten_like_sbi(eshiten_host, monkeypatch, capsys, answer, uploaded):
    monkeypatch.setenv('BRISK_ESHITEN_COOKIES', '{"s": "v"}')
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO(answer))
    cli.main(['live', '--site', 'e-shiten', '--codes', '7203'])
    out = capsys.readouterr()
    assert json.loads(out.out.splitlines()[-1])['last_price'] == 10160.0
    assert 'Upload automatically to your private archive?' in out.err
    assert len(eshiten_host['market']) == len(eshiten_host['timing']) == uploaded
    cli.main(['live', '--site', 'e-shiten', '--no-upload-market-data'])
    assert len(eshiten_host['market']) == uploaded and len(eshiten_host['timing']) == 2 * uploaded
    monkeypatch.setenv('BRISK_UPLOAD', '0')
    cli.main(['live', '--site', 'e-shiten'])
    assert len(eshiten_host['timing']) == 2 * uploaded, 'the environment opt-out keeps even timing local'
    with pytest.raises(SystemExit):
        cli.main(['live', '--site', 'demo', '--upload-market-data'])
    assert '--upload-market-data is for a broker' in capsys.readouterr().err
    with pytest.raises(SystemExit):
        cli.main(['live', '--site', 'sbi', '--cache', '.'])
    assert '--cache is for the demo' in capsys.readouterr().err
