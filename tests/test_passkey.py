"""Passkey sign-in: stores, the Python side of the helper protocol, site selection, the CLI, and full-stack runs in a real Chrome."""
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import time
import urllib.request

import pytest

import briskapi
import briskapi.cli as cli
from briskapi import brisk, passkey, sites

REPO = Path(__file__).resolve().parent.parent
posix_only = pytest.mark.skipif(os.name != 'posix', reason='uses a POSIX shell script or POSIX file modes')
windows_only = pytest.mark.skipif(sys.platform != 'win32', reason='needs Windows')


def assert_private(path, mode):
    """Owner-only modes exist on POSIX; on Windows a file's privacy comes from its folder's ACL."""
    if os.name == 'posix':
        assert stat.S_IMODE(path.stat().st_mode) == mode
PASSKEY = {'credentialId': 'Y3JlZA==', 'isResidentCredential': True, 'rpId': 'sbisec.co.jp', 'privateKey': 'PRIVATE-KEY-MATERIAL',
           'userHandle': 'AQID', 'signCount': 4, 'userName': 'trader'}
RECORD = {'site': 'sbi', 'credential': PASSKEY}


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.delenv('BRISK_PASSKEY_STORE', raising=False)
    monkeypatch.setattr(brisk, '_client', None)


# ---- stores ----

def test_file_store_keeps_the_saved_login_in_an_owner_only_file(tmp_path):
    store = passkey.FileStore(tmp_path / 'deep' / 'passkey.json')
    assert store.load() is None
    store.save(RECORD)
    assert store.load() == RECORD
    assert_private(store.path, 0o600)
    assert_private(store.path.parent, 0o700)
    store.save({**RECORD, 'site': 'matsui'})
    assert store.load()['site'] == 'matsui'
    assert [p.name for p in store.path.parent.iterdir()] == ['passkey.json']  # no temporary file left behind
    store.delete()
    store.delete()
    assert store.load() is None
    assert passkey.FileStore().path == tmp_path / 'config' / 'brisk' / 'passkey.json'


FAKE_SECURITY = r'''#!/bin/sh
dir="$FAKE_SECURITY_DIR"
echo "$@" >> "$dir/argv"
case "$1" in
  -i)
    read -r line
    echo "$line" >> "$dir/stdin"
    if [ -z "$FAKE_SECURITY_FAIL" ]; then printf '%s' "${line##* }" > "$dir/item"; else echo "add-generic-password: returned -25299"; fi ;;
  find-generic-password)
    [ -n "$FAKE_SECURITY_BROKEN" ] && exit 1
    if [ -f "$dir/item" ]; then cat "$dir/item"; echo; else exit 44; fi ;;
  delete-generic-password) rm -f "$dir/item" ;;
esac
'''


@pytest.fixture
def keychain(tmp_path, monkeypatch):
    script = tmp_path / 'security'
    script.write_text(FAKE_SECURITY)
    script.chmod(0o755)
    (tmp_path / 'kc').mkdir()
    monkeypatch.setenv('FAKE_SECURITY_DIR', str(tmp_path / 'kc'))
    return passkey.KeychainStore(security=str(script)), tmp_path / 'kc'


@posix_only
def test_keychain_store_never_puts_the_passkey_on_a_command_line(keychain):
    store, kc = keychain
    assert store.load() is None
    store.save(RECORD)
    assert store.load() == RECORD
    argv = (kc / 'argv').read_text(encoding='utf-8')
    assert 'PRIVATE-KEY-MATERIAL' not in argv
    assert 'add-generic-password' not in argv, 'a write on the command line would show the secret to other users'
    assert '-i' in argv.splitlines()
    assert all(line.startswith(('-i', 'find-generic-password')) for line in argv.splitlines())
    written = (kc / 'stdin').read_text(encoding='utf-8')
    assert written.startswith('add-generic-password -U -s briskapi-brisk-passkey -a default -w ')
    assert 'PRIVATE-KEY-MATERIAL' not in written  # base64 of the JSON, not the text itself
    store.delete()
    assert store.load() is None


@posix_only
def test_keychain_store_checks_that_the_write_really_happened(keychain, monkeypatch):
    store, _ = keychain
    monkeypatch.setenv('FAKE_SECURITY_FAIL', '1')
    with pytest.raises(passkey.PasskeyError, match=r'Could not save the passkey to the Keychain \(error -25299\)'):
        store.save(RECORD)
    monkeypatch.delenv('FAKE_SECURITY_FAIL')
    store.save(RECORD)
    monkeypatch.setenv('FAKE_SECURITY_BROKEN', '1')
    with pytest.raises(passkey.PasskeyError, match=r'Could not read the passkey from the Keychain \(security exit 1\)'):
        store.load()


@posix_only
def test_keychain_store_rejects_foreign_items_and_unsafe_names(keychain):
    store, kc = keychain
    (kc / 'item').write_text('not base64 json!')
    with pytest.raises(passkey.PasskeyError, match='not a passkey saved by briskapi'):
        store.load()
    for name in ('a b', 'x;rm', '', 'a\nb'):
        with pytest.raises(passkey.PasskeyError, match='may only use letters'):
            passkey.KeychainStore(service=name)
        with pytest.raises(passkey.PasskeyError, match='may only use letters'):
            passkey.KeychainStore(account=name)


def test_default_store_follows_the_platform_and_the_environment(monkeypatch):
    monkeypatch.setattr(passkey.sys, 'platform', 'darwin')
    assert isinstance(passkey.default_store(), passkey.KeychainStore)
    monkeypatch.setattr(passkey.sys, 'platform', 'win32')
    assert isinstance(passkey.default_store(), passkey.CredentialManagerStore)
    monkeypatch.setattr(passkey.sys, 'platform', 'linux')
    with pytest.raises(passkey.PasskeyError, match='BRISK_PASSKEY_STORE=file'):
        passkey.default_store()
    monkeypatch.setenv('BRISK_PASSKEY_STORE', 'file')
    assert isinstance(passkey.default_store(), passkey.FileStore)
    monkeypatch.setenv('BRISK_PASSKEY_STORE', 'keychain')
    with pytest.raises(passkey.PasskeyError, match='needs macOS'):
        passkey.default_store()
    monkeypatch.setenv('BRISK_PASSKEY_STORE', 'wincred')
    with pytest.raises(passkey.PasskeyError, match='needs Windows'):
        passkey.default_store()


class FakeCredApi:
    """Credential Manager's three calls, in memory, raising the errors the real ones would."""

    def __init__(self):
        self.items, self.fail, self.drop_writes = {}, {}, False

    def write(self, target, user, blob):
        if 'write' in self.fail:
            raise OSError(self.fail['write'], 'write failed')
        if not self.drop_writes:
            self.items[target] = (user, blob)

    def read(self, target):
        if 'read' in self.fail:
            raise OSError(self.fail['read'], 'read failed')
        return self.items[target][1] if target in self.items else None

    def delete(self, target):
        if 'delete' in self.fail:
            raise OSError(self.fail['delete'], 'delete failed')
        self.items.pop(target, None)


def test_credential_manager_store_keeps_the_saved_login_as_one_entry():
    api = FakeCredApi()
    store = passkey.CredentialManagerStore(api=api)
    assert store.load() is None and store.description == 'Windows Credential Manager entry briskapi-brisk-passkey'
    store.save(RECORD)
    assert store.load() == RECORD
    user, blob = api.items['briskapi-brisk-passkey']
    assert user == 'default' and json.loads(blob) == RECORD
    store.save({**RECORD, 'site': 'monex'})
    assert store.load()['site'] == 'monex' and len(api.items) == 1
    store.delete()
    store.delete()
    assert store.load() is None


def test_credential_manager_store_reports_failures_with_the_error_code():
    api = FakeCredApi()
    store = passkey.CredentialManagerStore(api=api)
    api.fail = {'write': 5}
    with pytest.raises(passkey.PasskeyError, match=r'Could not save the passkey to Credential Manager \(error 5\)'):
        store.save(RECORD)
    api.fail = {'read': 1312}
    with pytest.raises(passkey.PasskeyError, match=r'Could not read the passkey from Credential Manager \(error 1312\)'):
        store.load()
    api.fail = {'delete': 5}
    with pytest.raises(passkey.PasskeyError, match=r'Could not delete the passkey from Credential Manager \(error 5\)'):
        store.delete()
    api.fail = {}
    api.drop_writes = True
    with pytest.raises(passkey.PasskeyError, match=r'^Could not save the passkey to Credential Manager$'):
        store.save(RECORD)
    big = {'site': 'sbi', 'credential': {**PASSKEY, 'privateKey': 'x' * 3000}}
    with pytest.raises(passkey.PasskeyError, match=r'too large for one Credential Manager entry \(2560\)'):
        passkey.CredentialManagerStore(api=FakeCredApi()).save(big)
    api.drop_writes = False
    api.items['briskapi-brisk-passkey'] = ('default', b'not json')
    with pytest.raises(passkey.PasskeyError, match='not a passkey saved by briskapi'):
        store.load()
    for name in ('a b', 'x;y', ''):
        with pytest.raises(passkey.PasskeyError, match='may only use letters'):
            passkey.CredentialManagerStore(target=name)


@windows_only
def test_credential_manager_for_real():
    store = passkey.CredentialManagerStore(target=f'briskapi-test-{os.getpid()}')
    try:
        assert store.load() is None
        store.save(RECORD)
        assert store.load() == RECORD
        store.save({**RECORD, 'site': 'monex'})
        assert store.load()['site'] == 'monex'
    finally:
        store.delete()
    assert store.load() is None
    store.delete()


# ---- the helper protocol, against a scripted helper ----

FAKE_HELPER = r'''
const fs = require('fs');
const readline = require('readline');
const scenario = process.env.FAKE_SCENARIO;
const out = (m) => process.stdout.write(JSON.stringify(m) + '\n', () => { if (m.type === 'result') process.exit(0); });
const rl = readline.createInterface({ input: process.stdin });
const fresh = { credentialId: 'bmV3', isResidentCredential: true, rpId: 'sbisec.co.jp', privateKey: 'PRIV-NEW', userHandle: 'AQID', signCount: 1, userName: 'trader' };
let request = null;
rl.on('line', (line) => {
  if (!request) {
    request = JSON.parse(line);
    fs.writeFileSync(process.env.FAKE_LOG, JSON.stringify({ argv: process.argv.slice(2), request, pid: process.pid }));
    const credential = request.credential && { ...request.credential, signCount: request.credential.signCount + 1 };
    if (scenario === 'login-ok') { out({ type: 'credential', credential }); out({ type: 'result', ok: true, cookies: { session_x: 'cookie-value', other: 'o' } }); }
    else if (scenario === 'japanese') { out({ type: 'credential', credential: { ...credential, userName: '取引 太郎' } }); out({ type: 'result', ok: true, cookies: { session_x: 'v' } }); }
    else if (scenario === 'noise') { console.log('some stray output'); out({ type: 'credential', credential }); out({ type: 'result', ok: true, cookies: { session_x: 'v' } }); }
    else if (scenario === 'login-refused') { out({ type: 'credential', credential }); out({ type: 'result', ok: false, error: 'The login was refused' }); }
    else if (scenario === 'crash') { out({ type: 'credential', credential }); process.exit(3); }
    else if (scenario === 'hang') { out({ type: 'credential', credential }); setInterval(() => {}, 1000); }
    else if (scenario === 'eof-aware') { out({ type: 'credential', credential }); rl.on('close', () => { fs.writeFileSync(process.env.FAKE_LOG + '.eof', 'eof'); process.exit(0); }); setInterval(() => {}, 1000); }
    else if (scenario === 'enroll-ok') { out({ type: 'credential', credential: fresh }); out({ type: 'registered' }); }
    else if (scenario === 'enroll-refused') { out({ type: 'credential', credential: fresh }); out({ type: 'result', ok: false, error: 'No registration happened' }); }
    else if (scenario === 'enroll-empty') { out({ type: 'result', ok: true }); }
    else if (scenario === 'early-exit') { process.exit(4); }
  } else if (line.trim() === 'done') {
    fs.writeFileSync(process.env.FAKE_LOG + '.done', 'done');
    out({ type: 'credential', credential: { ...fresh, signCount: 2 } });
    out({ type: 'result', ok: true });
  }
});
'''


@pytest.fixture
def helper(tmp_path, monkeypatch):
    script = tmp_path / 'fake_passkey.cjs'
    script.write_text(FAKE_HELPER, encoding='utf-8')
    monkeypatch.setattr(passkey, 'HELPER', script)
    monkeypatch.setattr(passkey, 'GRACE', 0.5)  # the scripted helpers either exit on EOF or are terminated after this
    log = tmp_path / 'helper.json'
    monkeypatch.setenv('FAKE_LOG', str(log))

    def scenario(name):
        monkeypatch.setenv('FAKE_SCENARIO', name)
        return log
    return scenario


def keeper(tmp_path):
    """Passkeys that keep each broker's login in a file of its own in tmp_path (<site>.json; passkey.json, the place before each had its own)."""
    return passkey.Passkeys(lambda site: passkey.FileStore(tmp_path / (f'{site}.json' if site else 'passkey.json')))


def saved(tmp_path, record=RECORD):
    store = keeper(tmp_path)
    if isinstance(record, dict) and isinstance(record.get('site'), str) and isinstance(record.get('credential'), dict):
        store.save(record['site'], record)
    else:   # no broker to keep it for: as the old single place had it
        passkey.FileStore(tmp_path / 'passkey.json').save(record)
    return store


def alive(pid):
    if sys.platform == 'win32':  # os.kill(pid, 0) would terminate the process there
        listing = subprocess.run(['tasklist', '/FI', f'PID eq {pid}', '/NH', '/FO', 'CSV'], capture_output=True, text=True).stdout
        return f'"{pid}"' in listing
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def wait_gone(pid):
    for _ in range(50):
        if not alive(pid):
            return
        time.sleep(0.1)
    os.kill(pid, 9)
    pytest.fail('the helper (and its Chrome) was left running')


def test_login_signs_in_saves_the_advanced_passkey_and_uses_the_cookies(tmp_path, helper):
    log = helper('login-ok')
    store = saved(tmp_path)
    signin = passkey.login(store=store, login_url='https://x.example/login', launch_url='https://x.example/brisk',
                           passkey_button='Passkey', headless=True, chrome=Path('/opt/chrome'), profile_dir=tmp_path / 'profile')
    assert store.load('sbi') == {'site': 'sbi', 'credential': {**PASSKEY, 'signCount': 5}}, 'the sign counter must be saved'
    assert signin.site.id == 'sbi' and signin.cookies == {'session_x': 'cookie-value', 'other': 'o'}
    assert signin.client.session.cookies == signin.cookies and brisk._default() is signin.client
    seen = json.loads(log.read_text(encoding='utf-8'))
    assert seen['argv'] == ['login'], 'nothing but the mode may be on the command line'
    request = seen['request']
    assert request['credential'] == PASSKEY and request['mode'] == 'login'
    assert {k: request[k] for k in ('loginUrl', 'launchUrl', 'passkeyButton', 'headless', 'chrome', 'profileDir', 'cookieHost',
                                    'cookiePrefix')} == {
        'loginUrl': 'https://x.example/login', 'launchUrl': 'https://x.example/brisk', 'passkeyButton': 'Passkey',
        'headless': True, 'chrome': str(Path('/opt/chrome')), 'profileDir': str(tmp_path / 'profile'), 'cookieHost': 'sbi.brisk.jp',
        'cookiePrefix': 'session_'}
    cookie_file = tmp_path / 'config' / 'brisk' / 'sbi-cookies.json'
    assert signin.saved_to == cookie_file
    assert json.loads(cookie_file.read_text(encoding='utf-8')) == signin.cookies
    assert_private(cookie_file, 0o600)


def test_the_site_supplies_the_defaults_and_the_chosen_site_is_remembered(tmp_path, helper):
    log = helper('login-ok')
    store = saved(tmp_path, {'site': 'matsui', 'credential': PASSKEY})
    passkey.login(store=store, remember=False)
    request = json.loads(log.read_text(encoding='utf-8'))['request']
    matsui = sites.BUILTIN['matsui']
    assert (request['loginUrl'], request['cookieHost'], request['passkeyButton'], request['cookiePrefix']) == (
        matsui.login_url, 'matsui.brisk.jp', 'パスキーでログイン', '')
    assert 'launchUrl' not in request and 'chrome' not in request and 'headless' not in request
    assert store.load('matsui')['site'] == 'matsui'


def test_a_site_without_a_data_client_keeps_its_cookies_in_a_file_and_leaves_the_sbi_client_alone(tmp_path, helper):
    helper('login-ok')
    mine = tmp_path / 'sites.json'   # a broker you add has no data client
    mine.write_text(json.dumps({'sites': [{'id': 'mine', 'name': 'Mine', 'login_url': 'https://broker.example/login',
                                           'cookie_host': 'mine.brisk.jp', 'passkey_button': 'パスキーでログイン'}]}))
    store = saved(tmp_path, {'site': 'mine', 'credential': PASSKEY})
    signin = passkey.login(store=store, sites_file=mine)
    assert signin.site.id == 'mine' and signin.client is None and brisk._client is None
    file = tmp_path / 'config' / 'brisk' / 'cookies' / 'mine.json'
    assert signin.saved_to == file and json.loads(file.read_text(encoding='utf-8')) == {'session_x': 'cookie-value', 'other': 'o'}
    assert_private(file, 0o600)
    assert_private(file.parent, 0o700)
    assert not (tmp_path / 'config' / 'brisk' / 'sbi-cookies.json').exists()
    assert passkey.login(store=store, remember=False, sites_file=mine).saved_to is None


def test_login_without_remember_keeps_the_sbi_cookies_in_memory(tmp_path, helper):
    helper('login-ok')
    signin = passkey.login(store=saved(tmp_path), remember=False)
    assert signin.saved_to is None and not (tmp_path / 'config' / 'brisk' / 'sbi-cookies.json').exists()
    assert brisk._default().session.cookies['session_x'] == 'cookie-value'


def test_cookies_of_another_site_are_never_given_to_the_sbi_client(tmp_path, helper):
    log = helper('login-ok')
    store = saved(tmp_path)
    for host in ('matsui.brisk.jp', 'brisk.jp', 'sbi.brisk.jp.evil.example'):
        edited = tmp_path / 'sites.json'
        edited.write_text(json.dumps({'sites': [{'id': 'sbi', 'cookie_host': host}]}))
        with pytest.raises(passkey.PasskeyError, match=r'never given to the SBI BRiSK client, which only talks to sbi\.brisk\.jp'):
            passkey.login(store=store, sites_file=edited)
    assert not log.exists(), 'the refusal must come before Chrome is started'
    assert store.load('sbi') == RECORD, 'no sign-in happened, so the counter is unchanged'
    assert brisk._client is None


ESHITEN = {'site': 'e-shiten', 'user_id': 'abc123', 'credential': PASSKEY}


def test_e_shiten_signs_in_with_the_saved_user_id_and_its_data_client_gets_the_cookies(tmp_path, helper):
    log = helper('login-ok')
    store = saved(tmp_path, ESHITEN)
    signin = passkey.login(store=store)
    request = json.loads(log.read_text(encoding='utf-8'))['request']
    eshiten = sites.BUILTIN['e-shiten']
    assert (request['loginUrl'], request['passkeyButton'], request['userField'], request['userId'], request['cookieHost'],
            request['cookiePrefix']) == (eshiten.login_url, 'パスキーを使う', 'input[name="kokyakuUserId"]', 'abc123',
                                         'next.e-shiten.brisk.jp', 'session_')
    assert store.load('e-shiten') == {**ESHITEN, 'credential': {**PASSKEY, 'signCount': 5}}, 'saving the counter must keep the user ID'
    assert signin.client.site == 'e-shiten' and signin.client.session.origin == 'https://next.e-shiten.brisk.jp'
    assert brisk._default() is signin.client
    assert signin.saved_to == brisk.cookies_path('e-shiten') and not brisk.cookies_path('sbi').exists()
    assert json.loads(signin.saved_to.read_text(encoding='utf-8')) == signin.cookies
    assert_private(signin.saved_to, 0o600)


def test_a_site_without_a_user_id_box_is_sent_none_and_a_missing_user_id_stops_before_chrome(tmp_path, helper):
    log = helper('login-ok')
    passkey.login(store=saved(tmp_path), remember=False)
    request = json.loads(log.read_text(encoding='utf-8'))['request']
    assert 'userField' not in request and 'userId' not in request
    log.unlink()
    store = saved(tmp_path, {'site': 'e-shiten', 'credential': PASSKEY})
    with pytest.raises(passkey.PasskeyError, match=r'Passkeys are saved for sbi, e-shiten: choose one with --site'):
        passkey.login(store=store)   # two brokers have one: which is for the person to say
    with pytest.raises(passkey.PasskeyError, match=r"Tachibana e-shiten's passkey sign-in asks for your user ID.*brisk enroll --site e-shiten --replace"):
        passkey.login(store=store, site='e-shiten')
    assert not log.exists()
    with pytest.raises(passkey.PasskeyError, match='unknown format'):
        passkey.login(store=saved(tmp_path / 'odd', {**ESHITEN, 'user_id': 123}))


def test_cookies_of_another_site_are_never_given_to_the_e_shiten_client(tmp_path, helper):
    log = helper('login-ok')
    store = saved(tmp_path, ESHITEN)
    edited = tmp_path / 'sites.json'
    for host in ('sbi.brisk.jp', 'e-shiten.brisk.jp', 'next.e-shiten.brisk.jp.evil.example'):
        edited.write_text(json.dumps({'sites': [{'id': 'e-shiten', 'cookie_host': host}]}))
        with pytest.raises(passkey.PasskeyError,
                           match=r'never given to the e-shiten BRiSK Next client, which only talks to next\.e-shiten\.brisk\.jp'):
            passkey.login(store=store, sites_file=edited)
    assert not log.exists() and store.load('e-shiten') == ESHITEN and brisk._client is None


def test_non_ascii_text_from_the_helper_is_read_as_utf_8_whatever_the_system_code_page(tmp_path, helper):
    # Node writes UTF-8; Python on Windows would otherwise read it as cp1252 and fail on bytes such as 0x81.
    helper('japanese')
    store = saved(tmp_path)
    passkey.login(store=store, remember=False)
    assert store.load('sbi')['credential']['userName'] == '取引 太郎'


def test_stray_helper_output_is_ignored(tmp_path, helper):
    helper('noise')
    assert passkey.login(store=saved(tmp_path), remember=False).cookies == {'session_x': 'v'}


def test_a_refused_login_is_an_error_but_the_passkey_counter_is_still_saved(tmp_path, helper):
    helper('login-refused')
    store = saved(tmp_path)
    with pytest.raises(passkey.PasskeyError, match='The login was refused'):
        passkey.login(store=store)
    assert store.load('sbi')['credential']['signCount'] == 5
    assert brisk._client is None


def test_a_helper_that_dies_is_reported_and_its_last_passkey_is_kept(tmp_path, helper):
    helper('crash')
    store = saved(tmp_path)
    with pytest.raises(passkey.PasskeyError, match=r'stopped unexpectedly \(exit 3\)'):
        passkey.login(store=store)
    assert store.load('sbi')['credential']['signCount'] == 5
    helper('early-exit')
    with pytest.raises(passkey.PasskeyError, match=r'stopped unexpectedly \(exit 4\)'):
        passkey.login(store=store)


def test_if_the_passkey_cannot_be_saved_the_helper_is_stopped(helper):
    log = helper('hang')

    class Broken:
        def enrolled(self, sites_file=None):
            return ['sbi']

        def load(self, site):
            return RECORD

        def save(self, site, record):
            raise passkey.PasskeyError('Keychain is locked')

    with pytest.raises(passkey.PasskeyError, match='Keychain is locked'):
        passkey.login(store=Broken())
    wait_gone(json.loads(log.read_text(encoding='utf-8'))['pid'])


def test_a_helper_that_understands_a_closed_stdin_is_stopped_gracefully(helper):
    log = helper('eof-aware')

    class Broken:
        def enrolled(self, sites_file=None):
            return ['sbi']

        def load(self, site):
            return RECORD

        def save(self, site, record):
            raise passkey.PasskeyError('Keychain is locked')

    with pytest.raises(passkey.PasskeyError, match='Keychain is locked'):
        passkey.login(store=Broken())
    assert Path(str(log) + '.eof').exists(), 'the helper must be asked to stop by closing its stdin, so it can close Chrome'
    wait_gone(json.loads(log.read_text(encoding='utf-8'))['pid'])


def test_login_needs_a_usable_saved_login_and_node(tmp_path, helper):
    with pytest.raises(passkey.PasskeyError, match='Run `brisk enroll` first'):
        passkey.login(store=keeper(tmp_path))
    for number, odd in enumerate(({'credential': PASSKEY}, {'site': 'sbi'}, PASSKEY, ['x'])):
        with pytest.raises(passkey.PasskeyError, match=r'unknown format; run `brisk enroll --replace`'):
            passkey.login(store=saved(tmp_path / f'odd{number}', odd))
    with pytest.raises(passkey.PasskeyError, match='No passkey is saved for monex. Run `brisk enroll --site monex` first'):
        passkey.login(store=saved(tmp_path), site='monex')
    with pytest.raises(sites.SiteError, match="Unknown site 'gone'"):
        passkey.login(store=saved(tmp_path / 'gone', {'site': 'gone', 'credential': PASSKEY}), site='gone')
    for call in (lambda: passkey.login(store=saved(tmp_path), runtime=str(tmp_path / 'missing')),
                 lambda: passkey.enroll(site='sbi', store=keeper(tmp_path / 'e'), runtime=str(tmp_path / 'missing'))):
        with pytest.raises(briskapi.BriskError, match='not found'):
            call()


def test_enroll_saves_the_chosen_site_with_the_final_passkey_after_the_user_confirms(tmp_path, helper):
    log = helper('enroll-ok')
    store = keeper(tmp_path)
    confirmed = []
    summary = passkey.enroll(site='matsui', store=store, confirm=lambda: confirmed.append(True))
    assert confirmed == [True]
    assert Path(str(log) + '.done').exists(), 'the helper must be told when to close Chrome'
    record = store.load('matsui')
    assert record['site'] == 'matsui' and record['credential']['signCount'] == 2 and record['credential']['privateKey'] == 'PRIV-NEW'
    assert summary == {'site': 'matsui', 'rp_id': 'sbisec.co.jp', 'user_name': 'trader', 'stored_in': str(tmp_path / 'matsui.json')}
    assert 'PRIV' not in json.dumps(summary)
    seen = json.loads(log.read_text(encoding='utf-8'))
    assert seen['argv'] == ['enroll'] and seen['request']['loginUrl'] == sites.BUILTIN['matsui'].login_url
    passkey.enroll(site='sbi', store=store, confirm=lambda: None, login_url='https://x.example/start')
    assert json.loads(log.read_text(encoding='utf-8'))['request']['loginUrl'] == 'https://x.example/start'
    assert store.enrolled() == ['sbi', 'matsui'], 'each broker keeps its own: enrolling another leaves matsui\'s alone'
    passkey.enroll(site='matsui', store=store, confirm=lambda: None, replace=True, login_url='https://x.example/again')
    assert json.loads(log.read_text(encoding='utf-8'))['request']['loginUrl'] == 'https://x.example/again'


def test_enroll_asks_which_broker_when_no_site_is_given(tmp_path, helper):
    log = helper('enroll-ok')
    store = keeper(tmp_path)
    asked = []
    passkey.enroll(store=store, confirm=lambda: None, choose=lambda found: asked.append(list(found)) or 'smbcnikko')
    assert asked == [['sbi', 'matsui', 'monex', 'smbcnikko', 'e-shiten']] and store.load('smbcnikko')['site'] == 'smbcnikko'
    store.delete('smbcnikko')
    log.unlink()
    with pytest.raises(passkey.PasskeyError, match=r'Choose your broker with --site \(see `brisk sites`\)'):
        passkey.enroll(store=store)  # no terminal in the tests, so it cannot ask
    with pytest.raises(sites.SiteError, match="Unknown site 'nowhere'"):
        passkey.enroll(site='nowhere', store=store)
    assert not log.exists() and store.enrolled() == []


def test_enroll_at_e_shiten_starts_at_its_first_sign_in_and_keeps_the_user_id_with_the_passkey(tmp_path, helper):
    log = helper('enroll-ok')
    store = keeper(tmp_path)
    summary = passkey.enroll(site='e-shiten', store=store, confirm=lambda: None, user_id=' abc123 ')
    record = store.load('e-shiten')
    assert record == {'site': 'e-shiten', 'user_id': 'abc123', 'credential': {**record['credential'], 'signCount': 2}}
    assert 'abc123' not in json.dumps(summary)
    request = json.loads(log.read_text(encoding='utf-8'))['request']
    assert request['loginUrl'] == 'https://tr2.e-shiten.jp/e-shiten' and 'userId' not in request, 'enrolling is done by hand'
    passkey.enroll(site='e-shiten', store=store, confirm=lambda: None, user_id='abc123', replace=True, login_url='https://x.example/')
    assert json.loads(log.read_text(encoding='utf-8'))['request']['loginUrl'] == 'https://x.example/'
    store.delete('e-shiten')
    log.unlink()
    for bad in ('', '  ', 'a\nb', 'x' * 101):
        with pytest.raises(passkey.PasskeyError, match='one line of text'):
            passkey.enroll(site='e-shiten', store=store, confirm=lambda: None, user_id=bad)
    with pytest.raises(passkey.PasskeyError, match=r"asks for your user ID: pass user_id= or enroll from a terminal"):
        passkey.enroll(site='e-shiten', store=store, confirm=lambda: None)  # no terminal in the tests, so it cannot ask
    assert not log.exists() and store.load('e-shiten') is None, 'nothing starts without the user ID'
    passkey.enroll(site='sbi', store=store, confirm=lambda: None, user_id='ignored')
    assert 'user_id' not in store.load('sbi'), 'a site without a user ID box keeps none'


def test_the_user_id_is_asked_on_stderr(monkeypatch, capsys):
    monkeypatch.setattr(passkey.sys, 'stdin', type('Tty', (), {'isatty': lambda self: True})())
    monkeypatch.setattr('builtins.input', lambda prompt='': prompt + 'abc123')
    assert passkey._ask_user_id(sites.BUILTIN['e-shiten']) == 'User ID: abc123'
    captured = capsys.readouterr()
    assert "Tachibana e-shiten's passkey sign-in asks for your user ID" in captured.err and captured.out == ''


def test_the_interactive_choice_takes_a_number_or_an_id(monkeypatch, capsys):
    monkeypatch.setattr(passkey.sys, 'stdin', type('Tty', (), {'isatty': lambda self: True})())
    for typed, expected in (('2', 'matsui'), (' Monex ', 'monex'), ('9', '9'), ('0', '0')):
        monkeypatch.setattr('builtins.input', lambda prompt='', typed=typed: typed)
        assert passkey._choose(sites.BUILTIN) == expected
    err = capsys.readouterr().err
    assert '1) SBI Securities (sbi)' in err and '4) SMBC Nikko Securities (smbcnikko)' in err


def test_enroll_does_not_replace_a_working_passkey_unless_asked_or_until_it_succeeds(tmp_path, helper):
    helper('enroll-ok')
    store = saved(tmp_path)
    with pytest.raises(passkey.PasskeyError, match=r'A passkey for sbi is already saved. Use --replace'):
        passkey.enroll(site='sbi', store=store, confirm=lambda: None)
    assert store.load('sbi') == RECORD
    helper('enroll-refused')
    with pytest.raises(passkey.PasskeyError, match='No registration happened'):
        passkey.enroll(site='sbi', store=store, confirm=lambda: None, replace=True)
    assert store.load('sbi') == RECORD, 'a failed enrollment must leave the old passkey alone'
    helper('enroll-empty')
    with pytest.raises(passkey.PasskeyError, match='No passkey was captured'):
        passkey.enroll(site='sbi', store=store, confirm=lambda: None, replace=True)
    helper('enroll-ok')
    passkey.enroll(site='sbi', store=store, confirm=lambda: None, replace=True)
    assert store.load('sbi')['credential']['privateKey'] == 'PRIV-NEW'
    passkey.enroll(site='monex', store=store, confirm=lambda: None)   # another broker needs no replace, and leaves sbi's alone
    assert store.load('monex')['site'] == 'monex' and store.load('sbi')['credential']['privateKey'] == 'PRIV-NEW'


def test_enroll_waits_for_a_person_and_stops_chrome_when_there_is_none(tmp_path, helper, monkeypatch):
    log = helper('enroll-ok')
    store = keeper(tmp_path)
    monkeypatch.setattr(passkey.sys, 'stdin', type('NoTty', (), {'isatty': lambda self: False})())
    with pytest.raises(passkey.PasskeyError, match='needs an interactive terminal'):
        passkey.enroll(site='sbi', store=store)
    assert store.load('sbi') is None
    wait_gone(json.loads(log.read_text(encoding='utf-8'))['pid'])


def test_confirm_prompts_on_stderr_and_waits_for_enter(monkeypatch, capsys):
    monkeypatch.setattr(passkey.sys, 'stdin', type('Tty', (), {'isatty': lambda self: True})())
    monkeypatch.setattr('builtins.input', lambda: 'ok')
    passkey._confirm()
    captured = capsys.readouterr()
    assert 'press Enter here' in captured.err and captured.out == ''


def test_forget_deletes_the_saved_login(tmp_path):
    store = saved(tmp_path)
    assert passkey.forget(store=store) == 'sbi'
    assert store.load('sbi') is None and passkey.forget(store=store) is None


def test_each_broker_has_a_store_of_its_own_on_every_platform(monkeypatch, tmp_path):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path))
    monkeypatch.delenv('BRISK_PASSKEY_STORE', raising=False)
    monkeypatch.setattr(passkey.sys, 'platform', 'darwin')
    sbi, eshiten, old = passkey.default_store('sbi'), passkey.default_store('e-shiten'), passkey.default_store()
    assert [(s.service, s.account) for s in (sbi, eshiten, old)] == [(passkey.SERVICE, 'sbi'), (passkey.SERVICE, 'e-shiten'),
                                                                     (passkey.SERVICE, 'default')]
    monkeypatch.setattr(passkey.sys, 'platform', 'win32')
    assert passkey.default_store('sbi').target == 'briskapi-brisk-passkey-sbi' and passkey.default_store().target == 'briskapi-brisk-passkey'
    monkeypatch.setenv('BRISK_PASSKEY_STORE', 'file')
    assert passkey.default_store('sbi').path == tmp_path / 'brisk' / 'passkey-sbi.json'
    assert passkey.default_store().path == tmp_path / 'brisk' / 'passkey.json'
    for bad in ('../x', 'SBI', '', 'a b', 'x/y'):
        with pytest.raises(passkey.PasskeyError, match='Not a broker id'):
            passkey.default_store(bad)
        with pytest.raises(passkey.PasskeyError, match='Not a broker id'):
            passkey.Passkeys(lambda site: passkey.FileStore(tmp_path / 'x.json')).load(bad)


def test_passkeys_of_several_brokers_sit_side_by_side_and_each_is_found_by_its_site(tmp_path):
    store = keeper(tmp_path)
    assert store.enrolled() == [] and store.load('sbi') is None
    store.save('monex', {**RECORD, 'site': 'monex'})
    store.save('sbi', RECORD)
    assert store.enrolled() == ['sbi', 'monex'], 'in the order `brisk sites` lists them'
    assert store.load('sbi') == RECORD and store.load('monex')['site'] == 'monex'
    assert store.description('sbi') == str(tmp_path / 'sbi.json')
    store.delete('sbi')
    assert store.enrolled() == ['monex'] and store.load('sbi') is None
    with pytest.raises(passkey.PasskeyError, match='unknown format'):
        passkey.FileStore(tmp_path / 'matsui.json').save({'site': 'matsui'})
        store.load('matsui')


def test_a_login_saved_before_each_broker_had_its_own_moves_to_its_broker(tmp_path):
    passkey.FileStore(tmp_path / 'passkey.json').save({'site': 'matsui', 'credential': PASSKEY})
    store = keeper(tmp_path)
    assert store.enrolled() == ['matsui']
    assert store.load('matsui') == {'site': 'matsui', 'credential': PASSKEY}
    assert not (tmp_path / 'passkey.json').exists() and (tmp_path / 'matsui.json').exists()
    store.save('sbi', RECORD)
    assert keeper(tmp_path).enrolled() == ['sbi', 'matsui'], 'and it stays moved'


def test_an_old_login_for_a_broker_that_has_its_own_is_dropped_and_one_that_cannot_move_is_still_read(tmp_path):
    passkey.FileStore(tmp_path / 'passkey.json').save({'site': 'sbi', 'credential': {**PASSKEY, 'signCount': 1}})
    passkey.FileStore(tmp_path / 'sbi.json').save({'site': 'sbi', 'credential': {**PASSKEY, 'signCount': 9}})
    assert keeper(tmp_path).load('sbi')['credential']['signCount'] == 9, 'the broker\'s own is the newer'
    assert not (tmp_path / 'passkey.json').exists()

    class CannotSave(passkey.FileStore):
        def save(self, record):
            raise passkey.PasskeyError('Keychain is locked')
    other = tmp_path / 'other'
    passkey.FileStore(other / 'passkey.json').save({'site': 'monex', 'credential': PASSKEY})
    stuck = passkey.Passkeys(lambda site: CannotSave(other / (f'{site}.json' if site else 'passkey.json')))
    assert stuck.load('monex') == {'site': 'monex', 'credential': PASSKEY} and stuck.enrolled() == ['monex']
    assert (other / 'passkey.json').exists(), 'it stays where it was'
    assert keeper(other).enrolled() == ['monex'] and not (other / 'passkey.json').exists(), 'and moves when it can'
    # saving the broker's own login replaces an old one that could not be moved
    again = tmp_path / 'again'
    passkey.FileStore(again / 'passkey.json').save({'site': 'monex', 'credential': PASSKEY})
    half = passkey.Passkeys(lambda site: passkey.FileStore(again / (f'{site}.json' if site else 'passkey.json')))
    half._migrated, half._legacy = True, {'site': 'monex', 'credential': PASSKEY}   # as after a move that failed
    half.save('monex', {'site': 'monex', 'credential': {**PASSKEY, 'signCount': 3}})
    assert not (again / 'passkey.json').exists() and half.load('monex')['credential']['signCount'] == 3
    half._legacy = {'site': 'monex', 'credential': PASSKEY}
    passkey.FileStore(again / 'passkey.json').save({'site': 'monex', 'credential': PASSKEY})
    half.delete('monex')
    assert not (again / 'passkey.json').exists() and half.load('monex') is None


def test_login_and_forget_say_which_broker_when_more_than_one_has_a_passkey(tmp_path, helper):
    helper('login-ok')
    store = saved(tmp_path)
    store.save('monex', {**RECORD, 'site': 'monex'})
    with pytest.raises(passkey.PasskeyError, match='Passkeys are saved for sbi, monex: choose one with --site'):
        passkey.login(store=store)
    with pytest.raises(passkey.PasskeyError, match='Passkeys are saved for sbi, monex: choose one with --site'):
        passkey.forget(store=store)
    assert passkey.forget(site='monex', store=store) == 'monex' and store.enrolled() == ['sbi']
    assert passkey.login(store=store, remember=False).site.id == 'sbi', 'with one left, it is the one'
    assert passkey.login(store=store, remember=False, site='sbi').site.id == 'sbi'


def test_signing_in_takes_this_computers_passkey_lock_and_gives_it_back(tmp_path, helper, monkeypatch):
    helper('login-ok')
    held = []

    @passkey.contextmanager
    def spy(path=None, poll=0.2):
        held.append('taken')
        try:
            yield
        finally:
            held.append('given back')
    monkeypatch.setattr(passkey, '_one_at_a_time', spy)
    passkey.login(store=saved(tmp_path), remember=False)
    assert held == ['taken', 'given back']


def test_two_sign_ins_go_one_after_the_other(tmp_path):
    import threading
    import time
    lock, order, inside, release = tmp_path / 'config' / 'passkey.lock', [], threading.Event(), threading.Event()

    def first():
        with passkey._one_at_a_time(lock, poll=0.01):
            order.append('first in')
            inside.set()
            release.wait(10)
            order.append('first out')

    def second():
        inside.wait(10)
        with passkey._one_at_a_time(lock, poll=0.01):
            order.append('second in')
    threads = [threading.Thread(target=first), threading.Thread(target=second)]
    for thread in threads:
        thread.start()
    inside.wait(10)
    time.sleep(0.3)
    assert order == ['first in'], 'the second waits'
    release.set()
    for thread in threads:
        thread.join(10)
    assert order == ['first in', 'first out', 'second in']
    with passkey._one_at_a_time(lock):   # given back, though the first left by the door
        pass
    with pytest.raises(RuntimeError):
        with passkey._one_at_a_time(lock):
            raise RuntimeError('a sign-in that failed')
    with passkey._one_at_a_time(lock):
        pass


def test_nothing_in_the_brisk_module_is_about_passkeys():
    assert not [name for name in dir(brisk) if 'passkey' in name.lower()]


# ---- command line ----

def test_cli_sites_lists_the_built_ins_and_your_own(tmp_path, capsys):
    cli.main(['sites'])
    lines = capsys.readouterr().out.splitlines()
    assert [line.split()[0] for line in lines] == ['sbi', 'matsui', 'monex', 'smbcnikko', 'e-shiten']
    assert [line.split()[0] for line in lines if 'data client: yes' in line] == ['sbi', 'matsui', 'monex', 'smbcnikko', 'e-shiten']
    assert 'next.e-shiten.brisk.jp' in lines[-1]
    path = tmp_path / 'config' / 'brisk' / 'sites.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'sites': [{'id': 'mine', 'login_url': 'https://m.example/', 'cookie_host': 'mine.brisk.jp'}]}))
    cli.main(['sites'])
    last = capsys.readouterr().out.splitlines()[-1]
    assert last.split()[0] == 'mine' and 'mine.brisk.jp' in last and 'data client: no' in last and last.endswith('sites.json')


def test_cli_enroll_login_and_forget(monkeypatch, capsys):
    calls = []
    sbi_site, matsui_site = sites.BUILTIN['sbi'], sites.BUILTIN['matsui']
    results = iter([passkey.SignIn(sbi_site, {}, object(), Path('/c/sbi-cookies.json')), passkey.SignIn(matsui_site, {}, None, None),
                    passkey.SignIn(matsui_site, {}, None, None)])
    monkeypatch.setattr(passkey, 'enroll', lambda **kw: calls.append(('enroll', kw)) or {'site': 'matsui', 'stored_in': 'kc'})
    monkeypatch.setattr(passkey, 'login', lambda **kw: calls.append(('login', kw)) or next(results))
    monkeypatch.setattr(passkey, 'forget', lambda **kw: calls.append(('forget', kw)))
    cli.main(['enroll', '--site', 'matsui', '--login-url', 'https://x/', '--chrome', '/c', '--profile-dir', '/p', '--replace'])
    assert calls[-1] == ('enroll', {'site': 'matsui', 'login_url': 'https://x/', 'chrome': Path('/c'), 'profile_dir': Path('/p'), 'replace': True})
    assert json.loads(capsys.readouterr().out) == {'site': 'matsui', 'stored_in': 'kc'}
    cli.main(['enroll'])
    assert calls[-1][1]['site'] is None and calls[-1][1]['replace'] is False
    capsys.readouterr()
    cli.main(['login'])
    assert calls[-1] == ('login', {'remember': True, 'login_url': None, 'launch_url': None, 'passkey_button': None, 'chrome': None,
                                   'profile_dir': None, 'headless': False, 'site': None})
    err = capsys.readouterr().err
    assert f'Signed in to SBI Securities; session cookies saved to {Path("/c/sbi-cookies.json")}.' in err and 'no data client' not in err
    cli.main(['login', '--no-remember', '--headless', '--launch-url', 'https://b/', '--passkey-button', 'Passkey'])
    assert calls[-1][1]['remember'] is False and calls[-1][1]['headless'] is True and calls[-1][1]['launch_url'] == 'https://b/'
    err = capsys.readouterr().err
    assert 'Signed in to Matsui Securities; session cookies not saved.' in err and 'no data client for this site yet' in err
    cli.main(['login', '--site', 'e-shiten'])
    assert calls[-1][1]['site'] == 'e-shiten'
    capsys.readouterr()
    cli.main(['forget'])
    assert calls[-1] == ('forget', {'site': None})
    assert 'stays registered at your broker' in capsys.readouterr().err
    monkeypatch.setattr(passkey, 'forget', lambda **kw: calls.append(('forget', kw)) or kw['site'])
    cli.main(['forget', '--site', 'sbi'])
    assert calls[-1] == ('forget', {'site': 'sbi'})
    assert 'Passkey for sbi deleted here' in capsys.readouterr().err


def test_no_command_names_a_broker(capsys):
    with pytest.raises(SystemExit):
        cli.main(['sbi', 'login'])
    assert 'invalid choice' in capsys.readouterr().err


def test_cli_reports_passkey_errors_without_a_traceback(monkeypatch):
    monkeypatch.setenv('BRISK_PASSKEY_STORE', 'file')
    with pytest.raises(SystemExit) as stopped:
        cli.main(['login'])
    assert str(stopped.value.code) == 'brisk: error: No passkey is saved. Run `brisk enroll` first'
    with pytest.raises(SystemExit) as stopped:
        cli.main(['enroll', '--site', 'nowhere'])
    assert str(stopped.value.code).startswith("brisk: error: Unknown site 'nowhere'")


# ---- everything together: Python, the Node host, a real Chrome and a fake broker ----

def chrome_available():
    done = subprocess.run(['node', '-e', f'require({str(REPO / "briskapi/decoder/passkey.cjs")!r}).findChrome()'], capture_output=True)
    return done.returncode == 0


needs_chrome = pytest.mark.skipif(shutil.which('node') is None or not chrome_available(), reason='needs Node and Chrome')


class FakeBroker:
    """The Node fake SBI as a subprocess: a main site plus a BRiSK host, which verifies WebAuthn like a real site."""

    def __enter__(self):
        self.process = subprocess.Popen(['node', str(REPO / 'tools' / 'brisk_mock' / 'fake_sbi_passkey.cjs')],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.origins = json.loads(self.process.stdout.readline())
        return self

    def __exit__(self, *exc):
        self.process.stdin.close()
        self.process.wait(timeout=15)

    def state(self):
        # Chrome resolves *.localhost itself; Python's resolver on Windows does not, so ask the loopback address.
        with urllib.request.urlopen(self.origins['mainOrigin'].replace('main.localhost', '127.0.0.1') + '/__state', timeout=10) as response:
            return json.load(response)

    def registered(self):
        for _ in range(200):
            if self.state()['registered']:
                return
            time.sleep(0.05)
        raise AssertionError('the site never saw the registration')


def define(tmp_path, *entries):
    path = tmp_path / 'config' / 'brisk' / 'sites.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'sites': list(entries)}))


@needs_chrome
def test_full_stack_a_site_you_define_enroll_then_login(tmp_path):
    with FakeBroker() as broker:
        main = broker.origins['mainOrigin']
        define(tmp_path, {'id': 'fake', 'name': 'Fake Broker', 'login_url': main + '/login', 'cookie_host': 'brisk.localhost',
                          'passkey_button': 'パスキー認証でログイン', 'cookie_prefix': 'session_'})
        store = keeper(tmp_path)
        summary = passkey.enroll(site='fake', store=store, login_url=main + '/enroll', headless=True, confirm=broker.registered)
        assert summary['site'] == 'fake' and summary['rp_id'] == 'main.localhost'
        enrolled = store.load('fake')['credential']
        assert enrolled['privateKey'] and enrolled['isResidentCredential'] is True

        signin = passkey.login(store=store, headless=True)
        seen = broker.state()
        assert signin.site.id == 'fake' and signin.client is None and brisk._client is None
        assert signin.cookies['session_fake'] == seen['briskCookieValue'] and 'main_session' not in signin.cookies
        assert json.loads(signin.saved_to.read_text(encoding='utf-8')) == signin.cookies and signin.saved_to.name == 'fake.json'
        assert seen['accepted'] == 1
        assert store.load('fake')['credential']['signCount'] > enrolled['signCount']
        assert store.load('fake')['credential']['signCount'] == seen['counter'], 'the saved counter must be the one the site last saw'

        again = passkey.login(store=store, headless=True, remember=False)
        assert broker.state()['accepted'] == 2 and again.cookies['session_fake'] == broker.state()['briskCookieValue']


@needs_chrome
def test_full_stack_sbi_signs_in_and_its_data_client_gets_the_cookies(tmp_path, monkeypatch):
    monkeypatch.setitem(brisk.SITES, 'sbi', brisk.SITES['sbi']._replace(origin='https://brisk.localhost'))
    with FakeBroker() as broker:
        main = broker.origins['mainOrigin']
        define(tmp_path, {'id': 'sbi', 'login_url': main + '/login', 'cookie_host': 'brisk.localhost'})
        store = keeper(tmp_path)
        passkey.enroll(site='sbi', store=store, login_url=main + '/enroll', headless=True, confirm=broker.registered)
        signin = passkey.login(store=store, headless=True)
        assert signin.site.id == 'sbi' and brisk._default() is signin.client
        assert signin.client.session.cookies['session_fake'] == broker.state()['briskCookieValue']
        assert signin.saved_to == brisk.cookies_path() and broker.state()['accepted'] == 1


@needs_chrome
def test_full_stack_e_shiten_types_the_user_id_signs_in_through_its_popup_and_its_data_client_gets_the_cookies(tmp_path, monkeypatch):
    monkeypatch.setitem(brisk.SITES, 'e-shiten', brisk.SITES['e-shiten']._replace(origin='https://brisk.localhost'))
    with FakeBroker() as broker:
        main = broker.origins['mainOrigin']
        define(tmp_path, {'id': 'e-shiten', 'login_url': main + '/eshiten/login', 'enroll_url': main + '/enroll',
                          'cookie_host': 'brisk.localhost'})
        store = keeper(tmp_path)
        passkey.enroll(site='e-shiten', store=store, headless=True, confirm=broker.registered, user_id='abc123')
        signin = passkey.login(store=store, headless=True)
        seen = broker.state()
        assert seen['userIds'] == ['abc123'] and seen['accepted'] == 1 and seen['refusals'] == []
        assert signin.site.id == 'e-shiten' and signin.client.site == 'e-shiten' and brisk._default() is signin.client
        assert signin.client.session.cookies['session_fake'] == seen['briskCookieValue']
        assert signin.saved_to == brisk.cookies_path('e-shiten')
        assert store.load('e-shiten')['credential']['signCount'] == seen['counter'] and store.load('e-shiten')['user_id'] == 'abc123'
