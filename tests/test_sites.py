"""The list of BRiSK sites you can sign in to: built-in brokers and your own sites.json."""
import codecs
import json
from urllib.parse import urlsplit

import pytest

from briskapi import sbi, sites


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))


def write(tmp_path, document):
    path = tmp_path / 'sites.json'
    path.write_text(document if isinstance(document, str) else json.dumps(document, ensure_ascii=False), encoding='utf-8')
    return path


def test_the_built_in_sites_are_five_brokers_and_sbi_and_e_shiten_have_a_data_client():
    assert list(sites.BUILTIN) == ['sbi', 'matsui', 'monex', 'smbcnikko', 'e-shiten']
    for site in sites.BUILTIN.values():
        assert site.cookie_host.endswith('.brisk.jp')
        assert urlsplit(site.login_url).scheme == 'https' and site.passkey_button and site.source == 'built in'
    assert {site.id: site.client for site in sites.BUILTIN.values() if site.client} == {'sbi': 'sbi', 'e-shiten': 'e-shiten'}
    for site in (sites.BUILTIN['sbi'], sites.BUILTIN['e-shiten']):
        assert urlsplit(sbi.SITES[site.client].origin).hostname == site.cookie_host, 'cookies go only to the client of that host'
        assert site.cookie_prefix == 'session_'
    assert all(site.cookie_host == f'{site.id}.brisk.jp' for site in sites.BUILTIN.values() if not site.client)


def test_e_shiten_starts_enrolling_at_its_phone_sign_in_and_types_the_user_id_before_the_passkey():
    eshiten = sites.BUILTIN['e-shiten']
    assert (eshiten.login_url, eshiten.enroll_url) == ('https://tr2.e-shiten.jp/e-shiten?auth=passkey', 'https://tr2.e-shiten.jp/e-shiten')
    assert (eshiten.passkey_button, eshiten.user_field) == ('パスキーを使う', 'input[name="kokyakuUserId"]')
    assert all(site.enroll_url is None and site.user_field is None for site in sites.BUILTIN.values() if site.id != 'e-shiten')


def test_enroll_url_and_user_field_can_be_given_and_are_checked(tmp_path):
    path = write(tmp_path, {'sites': [{'id': 'mine', 'login_url': 'https://m.example/passkey', 'cookie_host': 'mine.brisk.jp',
                                       'enroll_url': 'https://m.example/first', 'user_field': '#user'}]})
    mine = sites.load_sites(path)['mine']
    assert (mine.enroll_url, mine.user_field) == ('https://m.example/first', '#user')
    with pytest.raises(sites.SiteError, match=r'\(mine\): enroll_url must be an http\(s\) URL'):
        sites.load_sites(write(tmp_path, {'sites': [{'id': 'mine', 'login_url': 'https://m.example/', 'cookie_host': 'm.example',
                                                     'enroll_url': 'javascript:alert(1)'}]}))
    for bad in ('', ' ', 7):
        with pytest.raises(sites.SiteError, match='user_field must be non-empty text, or null for none'):
            sites.load_sites(write(tmp_path, {'sites': [{'id': 'e-shiten', 'user_field': bad}]}))
    assert sites.load_sites(write(tmp_path, {'sites': [{'id': 'e-shiten', 'user_field': None}]}))['e-shiten'].user_field is None


def test_without_a_sites_file_the_built_ins_are_all_there_is(tmp_path):
    assert sites.load_sites(tmp_path / 'missing.json') == sites.BUILTIN
    assert sites.load_sites() == sites.BUILTIN  # the default path is under XDG_CONFIG_HOME
    assert sites.sites_path() == tmp_path / 'config' / 'brisk' / 'sites.json'


def test_a_new_site_needs_a_login_url_and_a_cookie_host(tmp_path):
    path = write(tmp_path, {'sites': [{'id': 'mybroker', 'login_url': 'https://broker.example/login',
                                       'cookie_host': 'mybroker.brisk.jp', 'passkey_button': 'Use a passkey',
                                       'cookie_prefix': 'sess', 'launch_url': 'https://mybroker.brisk.jp/'}]})
    found = sites.load_sites(path)
    assert list(found) == [*sites.BUILTIN, 'mybroker']
    mine = found['mybroker']
    assert (mine.name, mine.passkey_button, mine.cookie_prefix, mine.launch_url, mine.client, mine.source) == (
        'mybroker', 'Use a passkey', 'sess', 'https://mybroker.brisk.jp/', None, 'sites.json')
    path = write(tmp_path, {'sites': [{'id': 'bare', 'name': 'Bare', 'login_url': 'http://x.example/', 'cookie_host': 'x.example'}]})
    assert sites.load_sites(path)['bare'].passkey_button == 'パスキー'
    with pytest.raises(sites.SiteError, match=r'\(bare\): a new site needs login_url and cookie_host'):
        sites.load_sites(write(tmp_path, {'sites': [{'id': 'bare'}]}))
    with pytest.raises(sites.SiteError, match='a new site needs cookie_host'):
        sites.load_sites(write(tmp_path, {'sites': [{'id': 'bare', 'login_url': 'https://x.example/'}]}))


def test_a_sites_file_with_japanese_text_is_read_as_utf_8(tmp_path):
    path = write(tmp_path, {'sites': [{'id': 'matsui', 'passkey_button': 'パスキーで安全にログイン'}]})
    assert 'パスキーで安全にログイン' in path.read_bytes().decode('utf-8'), 'the fixture must hold real UTF-8, not escapes'
    assert sites.load_sites(path)['matsui'].passkey_button == 'パスキーで安全にログイン'


def test_a_built_in_site_is_edited_by_giving_only_the_keys_that_change(tmp_path):
    path = write(tmp_path, {'sites': [{'id': 'matsui', 'passkey_button': 'Passkey login', 'login_url': 'https://www.matsui.co.jp/new/'}]})
    edited = sites.load_sites(path)['matsui']
    assert (edited.passkey_button, edited.login_url) == ('Passkey login', 'https://www.matsui.co.jp/new/')
    assert edited.cookie_host == 'matsui.brisk.jp' and edited.name == 'Matsui Securities' and edited.source == 'sites.json'
    sbi = sites.load_sites(write(tmp_path, {'sites': [{'id': 'sbi', 'cookie_host': 'other.example'}]}))['sbi']
    assert sbi.client == 'sbi', 'editing a site must not remove its data client; the host guard handles the mismatch'


@pytest.mark.parametrize('encoding', ['utf-8', 'utf-8-sig', 'utf-16', 'utf-16-be-with-mark'])
def test_a_sites_file_is_read_whichever_way_a_windows_editor_saved_it(tmp_path, encoding):
    # utf-8-sig: Notepad and PowerShell 5.1 `-Encoding utf8`. utf-16: PowerShell 5.1 `>` and `Out-File`.
    text = json.dumps({'sites': [{'id': 'matsui', 'passkey_button': 'パスキーで安全にログイン'}]}, ensure_ascii=False)
    path = tmp_path / 'sites.json'
    data = codecs.BOM_UTF16_BE + text.encode('utf-16-be') if encoding == 'utf-16-be-with-mark' else text.encode(encoding)
    path.write_bytes(data)
    assert data.startswith({'utf-8': b'{', 'utf-8-sig': codecs.BOM_UTF8, 'utf-16': codecs.BOM_UTF16_LE,
                            'utf-16-be-with-mark': codecs.BOM_UTF16_BE}[encoding]), 'the fixture must carry the mark it claims'
    assert sites.load_sites(path)['matsui'].passkey_button == 'パスキーで安全にログイン'


def test_a_sites_file_that_is_not_text_is_refused_clearly(tmp_path):
    path = tmp_path / 'sites.json'
    path.write_bytes(b'\xff\xfe\x00\x00 not utf-16 or utf-8 \x81\x8d')
    with pytest.raises(sites.SiteError, match='is not valid JSON text'):
        sites.load_sites(path)


@pytest.mark.parametrize('document, message', [
    ('not json', 'is not valid JSON'),
    ([], 'must be an object with a "sites" list'),
    ({'sites': 'x'}, 'must be an object with a "sites" list'),
    ({'sites': ['x']}, 'each site must be an object'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'client': 'sbi'}]}, 'unknown key.*client'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'typo': 1}]}, 'unknown key.*typo'),
    ({'sites': [{'id': 'Bad Id', 'login_url': 'https://a/', 'cookie_host': 'a.x'}]}, 'id must be lower-case'),
    ({'sites': [{'login_url': 'https://a/', 'cookie_host': 'a.x'}]}, 'id must be lower-case'),
    ({'sites': [{'id': 'a', 'login_url': 'ftp://a/', 'cookie_host': 'a.x'}]}, 'login_url must be an http'),
    ({'sites': [{'id': 'a', 'login_url': 'nope', 'cookie_host': 'a.x'}]}, 'login_url must be an http'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'launch_url': 'file:///etc/passwd'}]}, 'launch_url must be an http'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'https://a.x/path'}]}, 'cookie_host must be a host name'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 5}]}, 'cookie_host must be a host name'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'name': ' '}]}, 'name must be non-empty text'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'passkey_button': 3}]}, 'passkey_button must be non-empty text'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'cookie_prefix': 3}]}, 'cookie_prefix must be text'),
])
def test_a_bad_sites_file_is_refused_with_a_clear_reason(tmp_path, document, message):
    with pytest.raises(sites.SiteError, match=message):
        sites.load_sites(write(tmp_path, document))


def test_an_unknown_site_lists_the_choices():
    with pytest.raises(sites.SiteError, match=r"Unknown site 'nope'. Choose one of: sbi, matsui, monex, smbcnikko, e-shiten \(see `brisk sites`\)"):
        sites.get_site(sites.BUILTIN, 'nope')
    assert sites.get_site(sites.BUILTIN, 'monex').name == 'Monex Securities'
