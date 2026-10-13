"""The BRiSK sites that `brisk enroll` / `brisk login` can sign in to: one entry per broker.

BRiSK is run for each broker on its own address (sbi.brisk.jp, matsui.brisk.jp, ...), behind that
broker's own login. A site entry says where the login page is, what its passkey button says and which
host the BRiSK session cookies belong to. Five are built in, taken from the brokers' own pages:

    sbi        SBI Securities
    matsui     Matsui Securities
    monex      Monex Securities
    smbcnikko  SMBC Nikko Securities
    e-shiten   Tachibana e-shiten (BRiSK Next at next.e-shiten.brisk.jp)

None has been checked against a live account, so each detail can be overridden, and you can add your
own entries (or correct a built-in one) in ~/.config/brisk/sites.json (C:\\Users\\you\\.config\\brisk\\sites.json
on Windows; XDG_CONFIG_HOME overrides the folder):

    {"sites": [{"id": "mybroker", "name": "My Broker", "login_url": "https://broker.example/login",
                "cookie_host": "mybroker.brisk.jp", "passkey_button": "Sign in with a passkey"}]}

Optional keys: passkey_button, cookie_prefix (a cookie name must start with it before sign-in counts as
done; empty accepts any), launch_url (opened after sign-in to start BRiSK), enroll_url (where `brisk
enroll` starts, when the first sign-in is not on the passkey login page) and user_field (a CSS selector
for a user ID box that must be filled before the passkey control works; enroll then asks for the ID and
keeps it with the passkey; null for none). An id that is already built in only replaces the keys you give.

SBI and e-shiten have a data client in briskapi (`briskapi.brisk`; `brisk live --sbi`, `brisk live --site
e-shiten`). For the other sites, signing in captures the BRiSK session cookies for your own use.
"""
from __future__ import annotations

import codecs
from dataclasses import dataclass, replace
import json
import os
from pathlib import Path
import re
import urllib.parse

from briskapi._recording import BriskError

_ID = re.compile(r'[a-z0-9][a-z0-9_-]*')
_HOST = re.compile(r'[a-z0-9]([a-z0-9.-]*[a-z0-9])?', re.IGNORECASE)
_KEYS = {'id', 'name', 'login_url', 'cookie_host', 'passkey_button', 'cookie_prefix', 'launch_url', 'enroll_url', 'user_field'}


class SiteError(BriskError):
    """A site is unknown or its definition is not valid."""


def config_dir() -> Path:
    return Path(os.environ.get('XDG_CONFIG_HOME') or Path.home() / '.config') / 'brisk'


def sites_path() -> Path:
    return config_dir() / 'sites.json'


@dataclass(frozen=True)
class Site:
    id: str
    name: str
    login_url: str
    cookie_host: str
    passkey_button: str = 'パスキー'
    cookie_prefix: str = ''
    launch_url: str | None = None
    enroll_url: str | None = None
    user_field: str | None = None
    client: str | None = None  # The briskapi.brisk site ('sbi', 'e-shiten') that serves this BRiSK. Never set from a file.
    source: str = 'built in'


BUILTIN = {site.id: site for site in (
    Site('sbi', 'SBI Securities', 'https://login.sbisec.co.jp/', 'sbi.brisk.jp', 'パスキー認証でログイン',
         cookie_prefix='session_', client='sbi'),
    Site('matsui', 'Matsui Securities', 'https://www.matsui.co.jp/login/', 'matsui.brisk.jp', 'パスキーでログイン'),
    Site('monex', 'Monex Securities', 'https://mst.monex.co.jp/pc/ITS/login/LoginIDPassword.jsp', 'monex.brisk.jp',
         'パスキーでログイン'),
    Site('smbcnikko', 'SMBC Nikko Securities', 'https://trade.smbcnikko.co.jp/Login/0/login/ipan_web/hyoji/',
         'smbcnikko.brisk.jp'),
    # The passkey form takes the user ID and runs the ceremony in a popup; a first sign-in is by phone.
    Site('e-shiten', 'Tachibana e-shiten', 'https://tr2.e-shiten.jp/e-shiten?auth=passkey', 'next.e-shiten.brisk.jp',
         'パスキーを使う', cookie_prefix='session_', enroll_url='https://tr2.e-shiten.jp/e-shiten',
         user_field='input[name="kokyakuUserId"]', client='e-shiten'),
)}


def _check_url(value, key, where):
    parts = urllib.parse.urlsplit(value) if isinstance(value, str) else None
    if not parts or parts.scheme not in ('http', 'https') or not parts.hostname:
        raise SiteError(f'{where}: {key} must be an http(s) URL')


def _entry(raw, where):
    if not isinstance(raw, dict):
        raise SiteError(f'{where}: each site must be an object')
    unknown = set(raw) - _KEYS
    if unknown:
        raise SiteError(f'{where}: unknown key(s) {", ".join(sorted(unknown))} (allowed: {", ".join(sorted(_KEYS))})')
    site_id = raw.get('id')
    if not isinstance(site_id, str) or not _ID.fullmatch(site_id):
        raise SiteError(f'{where}: id must be lower-case letters, digits, dashes or underscores')
    where = f'{where} ({site_id})'
    for key in ('name', 'passkey_button'):
        if key in raw and (not isinstance(raw[key], str) or not raw[key].strip()):
            raise SiteError(f'{where}: {key} must be non-empty text')
    if raw.get('user_field') is not None and (not isinstance(raw['user_field'], str) or not raw['user_field'].strip()):
        raise SiteError(f'{where}: user_field must be non-empty text, or null for none')
    if 'cookie_prefix' in raw and not isinstance(raw['cookie_prefix'], str):
        raise SiteError(f'{where}: cookie_prefix must be text')
    if 'cookie_host' in raw and (not isinstance(raw['cookie_host'], str) or not _HOST.fullmatch(raw['cookie_host'])):
        raise SiteError(f'{where}: cookie_host must be a host name such as x.brisk.jp, without a scheme or path')
    for key in ('login_url', 'launch_url', 'enroll_url'):
        if raw.get(key) is not None:
            _check_url(raw[key], key, where)
    return site_id, {key: value for key, value in raw.items() if key != 'id'}


def _read_text(path):
    """The text of a file you may have edited on Windows: UTF-8 (the usual case), UTF-8 with the byte-order
    mark that Notepad and PowerShell 5.1's `-Encoding utf8` write, or the UTF-16 that its `>` and `Out-File` write.
    Never the system code page: the button texts are Japanese."""
    data = path.read_bytes()
    for mark, codec in ((codecs.BOM_UTF8, 'utf-8-sig'), (codecs.BOM_UTF16_LE, 'utf-16'), (codecs.BOM_UTF16_BE, 'utf-16')):
        if data.startswith(mark):
            return data.decode(codec)
    return data.decode('utf-8')


def load_sites(path=None) -> dict:
    """The built-in sites, plus or edited by the entries in sites.json."""
    path = Path(path) if path else sites_path()
    sites = dict(BUILTIN)
    if not path.exists():
        return sites
    try:
        raw = json.loads(_read_text(path))
    except ValueError as error:
        raise SiteError(f'{path} is not valid JSON text ({error})') from error
    entries = raw.get('sites') if isinstance(raw, dict) else None
    if not isinstance(entries, list):
        raise SiteError(f'{path} must be an object with a "sites" list')
    for index, item in enumerate(entries):
        site_id, fields = _entry(item, f'{path} site {index + 1}')
        if site_id in sites:
            sites[site_id] = replace(sites[site_id], **fields, source=path.name)
        else:
            missing = [key for key in ('login_url', 'cookie_host') if key not in fields]
            if missing:
                raise SiteError(f'{path} ({site_id}): a new site needs {" and ".join(missing)}')
            sites[site_id] = Site(id=site_id, **{'name': site_id, **fields}, source=path.name)
    return sites


def get_site(sites, site_id) -> Site:
    if site_id not in sites:
        raise SiteError(f'Unknown site {site_id!r}. Choose one of: {", ".join(sites)} (see `brisk sites`)')
    return sites[site_id]
