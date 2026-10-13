"""The layout check's downloader, with local fake responses: pins, the master taken out of the capture, caching."""
import base64
import hashlib
import importlib.util
from io import BytesIO
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location('sbi_master_layout', Path(__file__).with_name('sbi_master_layout.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


SCHEDULE = {'morning_session_pre_open_time': 28800000000, 'morning_session_open_time': 32400000000,
            'morning_session_close_time': 41400000000, 'afternoon_session_pre_open_time': 43500000000,
            'afternoon_session_open_time': 45000000000, 'afternoon_session_pre_close_time': 55500000000,
            'afternoon_session_close_time': 55800000000, 'sq_jump_interval': 180}
SENT = {'issue_ids': [0, 5], 'update_numbers_from': [3678, 8678], 'update_numbers_to': [3680, 8681]}


def capture(master, token='v2.local.SECRET', reply=b'catch-up reply'):
    boot = {'ws_url': '/realtime/0?session=SECRET', 'identity': 'SECRET', 'schedule_info': SCHEDULE}
    entries = [{'request': {'url': 'https://sbi.brisk.jp/api/frontend/boot'}, 'response': {'content': {'text': token}}},
               {'request': {'url': f'https://sbi.brisk.jp/api/master/{hashlib.sha256(master).hexdigest()}'},
                'response': {'content': {'text': base64.b64encode(master).decode(), 'encoding': 'base64'}}},
               {'request': {'url': 'https://sbi.brisk.jp/api/app/boot'}, 'response': {'content': {'text': json.dumps(boot)}}},
               {'request': {'url': 'https://sbi.brisk.jp/api/stocks_update/0?date=2026-03-11',
                            'postData': {'text': json.dumps({**SENT, 'session': 'SECRET'})}},
                'response': {'content': {'text': base64.b64encode(reply).decode(), 'encoding': 'base64'}}}]
    return json.dumps({'log': {'entries': entries}}).encode()


def test_only_the_pinned_master_leaves_the_capture_and_files_are_cached(tmp_path, monkeypatch):
    master, wasm, glue, stream = b'm' * 180, b'\0asm', b'var CC;', b'stream'
    har = capture(master)
    files = {'har': har, 'wasm': wasm, 'js': glue, 'stream': stream}
    monkeypatch.setattr(module, 'MASTER', hashlib.sha256(master).hexdigest())
    monkeypatch.setattr(module, 'CATCH_UP', hashlib.sha256(b'catch-up reply').hexdigest())
    monkeypatch.setattr(module, 'CAPTURE', ('har', hashlib.sha256(har).hexdigest()))
    monkeypatch.setattr(module, 'STREAM', ('stream', hashlib.sha256(stream).hexdigest()))
    monkeypatch.setattr(module, 'DECODER', {'fita.wasm': ('wasm', hashlib.sha256(wasm).hexdigest()),
                                            'fita.js': ('js', hashlib.sha256(glue).hexdigest())})
    calls = []
    monkeypatch.setattr(module, 'urlopen', lambda url, timeout: calls.append(url) or BytesIO(files[url]))
    module.download(tmp_path)
    assert sorted(p.name for p in tmp_path.iterdir()) == ['fita.js', 'fita.wasm', 'master.dat', 'schedule_info.json',
                                                          'stocks_update.dat', 'stocks_update_request.json', 'stream.jsonl.gz']
    assert (tmp_path / 'master.dat').read_bytes() == master
    assert json.loads((tmp_path / 'schedule_info.json').read_text()) == SCHEDULE
    assert json.loads((tmp_path / 'stocks_update_request.json').read_text()) == SENT
    assert (tmp_path / 'stocks_update.dat').read_bytes() == b'catch-up reply'
    assert b'SECRET' not in b''.join(p.read_bytes() for p in tmp_path.iterdir()), 'the capture\'s session is never kept'
    module.download(tmp_path)
    assert sorted(calls) == ['har', 'js', 'stream', 'wasm']
    (tmp_path / 'fita.wasm').unlink()
    files['wasm'] = b'changed'
    with pytest.raises(ValueError, match='changed; re-audit'):
        module.download(tmp_path)
    assert not (tmp_path / 'fita.wasm').exists()


def test_a_capture_without_the_pinned_master_is_refused(monkeypatch):
    monkeypatch.setattr(module, 'MASTER', 'f' * 64)
    with pytest.raises(ValueError, match='no master file with the pinned hash'):
        module.master_from_capture(capture(b'other'))


def test_a_capture_without_the_pinned_catch_up_or_a_schedule_is_refused(monkeypatch):
    monkeypatch.setattr(module, 'CATCH_UP', 'f' * 64)
    with pytest.raises(ValueError, match='no catch-up reply with the pinned hash'):
        module.catch_up_from_capture(capture(b'm'))
    har = json.loads(capture(b'm'))
    har['log']['entries'][2]['response']['content']['text'] = json.dumps({'ws_url': 'x'})
    with pytest.raises(ValueError, match='no app boot with a complete schedule_info'):
        module.schedule_from_capture(json.dumps(har).encode())
