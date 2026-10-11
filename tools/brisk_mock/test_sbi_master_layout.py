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


def capture(master, token='v2.local.SECRET'):
    entries = [{'request': {'url': 'https://sbi.brisk.jp/api/frontend/boot'}, 'response': {'content': {'text': token}}},
               {'request': {'url': f'https://sbi.brisk.jp/api/master/{hashlib.sha256(master).hexdigest()}'},
                'response': {'content': {'text': base64.b64encode(master).decode(), 'encoding': 'base64'}}}]
    return json.dumps({'log': {'entries': entries}}).encode()


def test_only_the_pinned_master_leaves_the_capture_and_files_are_cached(tmp_path, monkeypatch):
    master, wasm, glue = b'm' * 180, b'\0asm', b'var CC;'
    har = capture(master)
    files = {'har': har, 'wasm': wasm, 'js': glue}
    monkeypatch.setattr(module, 'MASTER', hashlib.sha256(master).hexdigest())
    monkeypatch.setattr(module, 'CAPTURE', ('har', hashlib.sha256(har).hexdigest()))
    monkeypatch.setattr(module, 'DECODER', {'fita.wasm': ('wasm', hashlib.sha256(wasm).hexdigest()),
                                            'fita.js': ('js', hashlib.sha256(glue).hexdigest())})
    calls = []
    monkeypatch.setattr(module, 'urlopen', lambda url, timeout: calls.append(url) or BytesIO(files[url]))
    module.download(tmp_path)
    assert sorted(p.name for p in tmp_path.iterdir()) == ['fita.js', 'fita.wasm', 'master.dat']
    assert (tmp_path / 'master.dat').read_bytes() == master
    assert b'SECRET' not in b''.join(p.read_bytes() for p in tmp_path.iterdir()), 'the capture\'s session is never kept'
    module.download(tmp_path)
    assert sorted(calls) == ['har', 'js', 'wasm']
    (tmp_path / 'fita.wasm').unlink()
    files['wasm'] = b'changed'
    with pytest.raises(ValueError, match='changed; re-audit'):
        module.download(tmp_path)
    assert not (tmp_path / 'fita.wasm').exists()


def test_a_capture_without_the_pinned_master_is_refused(monkeypatch):
    monkeypatch.setattr(module, 'MASTER', 'f' * 64)
    with pytest.raises(ValueError, match='no master file with the pinned hash'):
        module.master_from_capture(capture(b'other'))
