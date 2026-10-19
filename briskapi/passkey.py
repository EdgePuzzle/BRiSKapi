"""Sign in to your broker's BRiSK with a passkey, through Chrome's virtual authenticator.

    brisk enroll      # once for each broker: pick it, sign in by hand, register a passkey in its settings
    brisk login       # each session: Chrome signs in with that passkey and the BRiSK cookies are kept
    brisk forget      # delete a saved passkey
    brisk sites       # the brokers you can pick (SBI, Matsui, Monex, SMBC Nikko, Tachibana e-shiten, or your own)

or from Python:

    from briskapi import passkey
    passkey.enroll(site="sbi")
    signin = passkey.login()          # signin.cookies; signin.client for a site with a data client (the five built in)

With passkeys saved for more than one broker, `login` and `forget` take the broker (`--site e-shiten`, `site="e-shiten"`).

How it works. A passkey is a key pair: the broker keeps the public half and your device keeps the
private half, which proves it is you by signing a challenge the site sends. Normally that private half
lives in your phone, laptop or password manager and needs a fingerprint or PIN to use. Here it lives
in a Chrome that briskapi starts, which has a *virtual authenticator*: software that plays the part of
the device and signs without asking. So:

  * enroll opens that Chrome at your broker's login page. You sign in as usual and, in the broker's
    security settings, register a passkey as you would on a new phone. The virtual authenticator
    receives it, briskapi saves it, and Chrome closes. Your phone's and password manager's passkeys
    are not touched; the broker's list of passkeys just shows one more entry, which you can delete.
  * login opens the same Chrome with the saved passkey loaded, goes to the login page, presses the
    passkey button and the virtual authenticator signs. You are signed in without a touch. Chrome
    then reads the BRiSK session cookies (only those of the BRiSK host), saves them and closes.
    A broker whose passkey form wants your user ID first (e-shiten) gets the one enroll asked for.

The saved login of a broker is the broker you chose plus the passkey (and that user ID, where one is needed). The
passkey's private key IS the credential: whoever has it can sign in to your account with no further
check, so it is kept in the macOS Keychain or
Windows Credential Manager (or, only if you set BRISK_PASSKEY_STORE=file, an owner-only file) and is never
put on a command line, in a log or in the repository. A passkey also carries a sign counter that a broker may check, so every change is
saved the moment it happens. Chrome runs under Node over a private pipe (briskapi/decoder/passkey.cjs)
with a temporary profile that is deleted afterwards. Chrome tells the site it is being automated, so a
broker may refuse it. See ARCHITECTURE.md for the details.
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.parse

from briskapi._recording import BriskError
from briskapi.sites import Site, config_dir, get_site, load_sites

HELPER = Path(__file__).resolve().parent / 'decoder' / 'passkey.cjs'
SERVICE = 'briskapi-brisk-passkey'
ACCOUNT = 'default'
SECURITY = '/usr/bin/security'
GRACE = 20.0  # seconds a helper gets to close Chrome, delete its profile and exit after its stdin is closed, before it is terminated
_NAME = re.compile(r'[A-Za-z0-9._-]+')


class PasskeyError(BriskError):
    """Passkey enrollment or sign-in failed."""


@dataclass
class SignIn:
    """The result of `login`: the site, its BRiSK cookies, and the data client if briskapi has one for it."""
    site: Site
    cookies: dict
    client: object = None
    saved_to: Path | None = None


class FileStore:
    """The saved login in an owner-only file. Weaker than the Keychain: anyone who can read your files can sign in."""

    def __init__(self, path=None):
        self.path = Path(path) if path else config_dir() / 'passkey.json'
        self.description = str(self.path)

    def load(self):
        return json.loads(self.path.read_text(encoding='utf-8')) if self.path.exists() else None

    def save(self, record):
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = self.path.with_name(self.path.name + '.tmp')
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(record, f)
        os.replace(temporary, self.path)

    def delete(self):
        self.path.unlink(missing_ok=True)


class KeychainStore:
    """The saved login in the macOS Keychain, through the `security` tool.

    `security add-generic-password -w SECRET` would put the secret on the command line, where other
    users can read it. `security -i` reads its commands from stdin instead, so only `security -i` is
    ever visible. The secret is base64 JSON, which needs no quoting.
    """

    def __init__(self, service=SERVICE, account=ACCOUNT, security=SECURITY):
        for name in (service, account):
            if not _NAME.fullmatch(name):
                raise PasskeyError('Keychain service and account names may only use letters, digits, dots, dashes and underscores')
        self.service, self.account, self.security = service, account, security
        self.description = f'macOS Keychain item {service}'

    def load(self):
        done = subprocess.run([self.security, 'find-generic-password', '-s', self.service, '-a', self.account, '-w'],
                              capture_output=True, text=True, encoding='utf-8')
        if done.returncode == 44:  # errSecItemNotFound
            return None
        if done.returncode != 0:
            raise PasskeyError(f'Could not read the passkey from the Keychain (security exit {done.returncode})')
        try:
            return json.loads(base64.b64decode(done.stdout.strip(), validate=True))
        except ValueError as error:
            raise PasskeyError('The Keychain item is not a passkey saved by briskapi') from error

    def save(self, record):
        secret = base64.b64encode(json.dumps(record).encode()).decode()
        done = subprocess.run([self.security, '-i'], capture_output=True, text=True, encoding='utf-8',
                              input=f'add-generic-password -U -s {self.service} -a {self.account} -w {secret}\n')
        # `security -i` exits 0 even when the command fails, so the write is checked by reading it back.
        if self.load() != record:
            code = re.search(r'returned (-?\d+)', done.stdout + done.stderr)
            raise PasskeyError('Could not save the passkey to the Keychain' + (f' (error {code.group(1)})' if code else ''))

    def delete(self):
        subprocess.run([self.security, 'delete-generic-password', '-s', self.service, '-a', self.account],
                       capture_output=True, text=True)


class _CredentialManager:
    """The few advapi32 calls Credential Manager needs, through ctypes (Windows only, no extra dependency)."""
    CRED_TYPE_GENERIC = 1
    CRED_PERSIST_LOCAL_MACHINE = 2  # this user on this machine; never roams
    ERROR_NOT_FOUND = 1168

    def __init__(self):
        import ctypes
        from ctypes import wintypes
        self.ctypes = ctypes

        class FILETIME(ctypes.Structure):
            _fields_ = [('dwLowDateTime', wintypes.DWORD), ('dwHighDateTime', wintypes.DWORD)]

        class CREDENTIAL(ctypes.Structure):
            _fields_ = [('Flags', wintypes.DWORD), ('Type', wintypes.DWORD), ('TargetName', wintypes.LPWSTR),
                        ('Comment', wintypes.LPWSTR), ('LastWritten', FILETIME), ('CredentialBlobSize', wintypes.DWORD),
                        ('CredentialBlob', ctypes.POINTER(ctypes.c_ubyte)), ('Persist', wintypes.DWORD),
                        ('AttributeCount', wintypes.DWORD), ('Attributes', ctypes.c_void_p),
                        ('TargetAlias', wintypes.LPWSTR), ('UserName', wintypes.LPWSTR)]

        self.CREDENTIAL = CREDENTIAL
        self.dll = dll = ctypes.WinDLL('advapi32', use_last_error=True)
        dll.CredWriteW.argtypes, dll.CredWriteW.restype = [ctypes.POINTER(CREDENTIAL), wintypes.DWORD], wintypes.BOOL
        dll.CredReadW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(ctypes.POINTER(CREDENTIAL))]
        dll.CredReadW.restype = wintypes.BOOL
        dll.CredDeleteW.argtypes, dll.CredDeleteW.restype = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD], wintypes.BOOL
        dll.CredFree.argtypes, dll.CredFree.restype = [ctypes.c_void_p], None

    def write(self, target, user, blob):
        ctypes = self.ctypes
        buffer = (ctypes.c_ubyte * len(blob)).from_buffer_copy(blob)
        credential = self.CREDENTIAL(Type=self.CRED_TYPE_GENERIC, TargetName=target, UserName=user, CredentialBlobSize=len(blob),
                                     CredentialBlob=ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)),
                                     Persist=self.CRED_PERSIST_LOCAL_MACHINE)
        if not self.dll.CredWriteW(ctypes.byref(credential), 0):
            raise ctypes.WinError(ctypes.get_last_error())

    def read(self, target):
        ctypes = self.ctypes
        pointer = ctypes.POINTER(self.CREDENTIAL)()
        if not self.dll.CredReadW(target, self.CRED_TYPE_GENERIC, 0, ctypes.byref(pointer)):
            error = ctypes.get_last_error()
            if error == self.ERROR_NOT_FOUND:
                return None
            raise ctypes.WinError(error)
        try:
            return ctypes.string_at(pointer.contents.CredentialBlob, pointer.contents.CredentialBlobSize)
        finally:
            self.dll.CredFree(pointer)

    def delete(self, target):
        ctypes = self.ctypes
        if not self.dll.CredDeleteW(target, self.CRED_TYPE_GENERIC, 0):
            error = ctypes.get_last_error()
            if error != self.ERROR_NOT_FOUND:
                raise ctypes.WinError(error)


def _code(error):
    return getattr(error, 'winerror', None) or error.errno


class CredentialManagerStore:
    """The saved login in Windows Credential Manager (this user, this machine), through advapi32."""
    MAX_BLOB = 5 * 512  # CRED_MAX_CREDENTIAL_BLOB_SIZE

    def __init__(self, target=SERVICE, user=ACCOUNT, api=None):
        for name in (target, user):
            if not _NAME.fullmatch(name):
                raise PasskeyError('Credential names may only use letters, digits, dots, dashes and underscores')
        self.target, self.user, self.api = target, user, api
        self.description = f'Windows Credential Manager entry {target}'

    def _api(self):
        if self.api is None:
            self.api = _CredentialManager()
        return self.api

    def load(self):
        try:
            blob = self._api().read(self.target)
        except OSError as error:
            raise PasskeyError(f'Could not read the passkey from Credential Manager (error {_code(error)})') from error
        if blob is None:
            return None
        try:
            return json.loads(blob.decode('utf-8'))
        except ValueError as error:
            raise PasskeyError('The Credential Manager entry is not a passkey saved by briskapi') from error

    def save(self, record):
        blob = json.dumps(record).encode('utf-8')
        if len(blob) > self.MAX_BLOB:
            raise PasskeyError(f'The passkey ({len(blob)} bytes) is too large for one Credential Manager entry ({self.MAX_BLOB})')
        try:
            self._api().write(self.target, self.user, blob)
        except OSError as error:
            raise PasskeyError(f'Could not save the passkey to Credential Manager (error {_code(error)})') from error
        if self.load() != record:
            raise PasskeyError('Could not save the passkey to Credential Manager')

    def delete(self):
        try:
            self._api().delete(self.target)
        except OSError as error:
            raise PasskeyError(f'Could not delete the passkey from Credential Manager (error {_code(error)})') from error


_SITE = re.compile(r'[a-z0-9][a-z0-9_-]*')


def default_store(site=None):
    """Where the saved login of `site` is kept on this system: a store for that broker alone (`site=None`: the one place a
    login was kept before each broker had its own, which `Passkeys` moves to its broker's)."""
    if site is not None and not _SITE.fullmatch(site):
        raise PasskeyError(f'Not a broker id: {site!r}')
    kind = os.environ.get('BRISK_PASSKEY_STORE') or {'darwin': 'keychain', 'win32': 'wincred'}.get(sys.platform, '')
    if kind == 'keychain':
        if sys.platform != 'darwin':
            raise PasskeyError('The Keychain store needs macOS')
        return KeychainStore(account=site or ACCOUNT)
    if kind == 'wincred':
        if sys.platform != 'win32':
            raise PasskeyError('The Credential Manager store needs Windows')
        return CredentialManagerStore(target=f'{SERVICE}-{site}' if site else SERVICE, user=site or ACCOUNT)
    if kind == 'file':
        return FileStore(config_dir() / (f'passkey-{site}.json' if site else 'passkey.json'))
    raise PasskeyError('There is no secure place for the passkey on this system. Set BRISK_PASSKEY_STORE=file to keep it '
                       'in an owner-only file (weaker: anyone who can read your files can sign in)')


def _checked(record):
    if not (isinstance(record, dict) and isinstance(record.get('site'), str) and isinstance(record.get('credential'), dict)
            and isinstance(record.get('user_id', ''), str)):
        raise PasskeyError('The saved passkey is in an unknown format; run `brisk enroll --replace`')
    return record


class Passkeys:
    """The saved logins, one for each broker (a site id, as in `brisk sites`), each in a store of its own: `load(site)`,
    `save(site, record)`, `delete(site)`, `enrolled()` (the sites that have one) and `description(site)` (where it is kept).
    `make(site)` gives a broker's store (default_store). A login saved before each broker had its own, one for whichever
    broker was enrolled, is moved to its broker's the first time anything is read."""

    def __init__(self, make=None):
        self._make = make or default_store
        self._migrated, self._legacy = False, None

    def _store(self, site):
        if not isinstance(site, str) or not _SITE.fullmatch(site):
            raise PasskeyError(f'Not a broker id: {site!r}')
        return self._make(site)

    def _migrate(self):
        if self._migrated:
            return
        self._migrated = True
        old = self._make(None)
        try:
            record = old.load()
        except PasskeyError:
            return   # an old item that cannot be read is for whoever can; the brokers' own are unaffected
        if record is None:
            return
        record = _checked(record)
        self._legacy = record   # read from here, should it not be moved
        try:
            target = self._store(record['site'])
            if target.load() is None:
                target.save(record)
            old.delete()
            self._legacy = None
        except PasskeyError:
            pass   # it stays where it was, and is moved at the next try

    def load(self, site):
        self._migrate()
        record = self._store(site).load()
        if record is None and self._legacy and self._legacy['site'] == site:
            return self._legacy
        return None if record is None else _checked(record)

    def save(self, site, record):
        self._migrate()
        self._store(site).save(record)
        if self._legacy and self._legacy['site'] == site:   # the old one is replaced
            try:
                self._make(None).delete()
                self._legacy = None
            except PasskeyError:
                pass

    def delete(self, site):
        self._migrate()
        self._store(site).delete()
        if self._legacy and self._legacy['site'] == site:
            self._make(None).delete()
            self._legacy = None

    def enrolled(self, sites_file=None):
        """The sites that have a saved login, in the order `brisk sites` lists them."""
        self._migrate()
        known = list(load_sites(sites_file))
        if self._legacy and self._legacy['site'] not in known:
            known.append(self._legacy['site'])
        return [site for site in known if self.load(site) is not None]

    def description(self, site):
        return self._store(site).description


def cookies_path(site_id) -> Path:
    """Where `login` keeps the BRiSK cookies of a site that has no data client (a site with one keeps them at brisk.cookies_path())."""
    return config_dir() / 'cookies' / f'{site_id}.json'


def _record(passkeys, site, sites_file=None):
    """The saved login of `site`, or of the only broker that has one."""
    if site is not None:
        chosen = get_site(load_sites(sites_file), site).id
        record = passkeys.load(chosen)
        if record is None:
            raise PasskeyError(f'No passkey is saved for {chosen}. Run `brisk enroll --site {chosen}` first')
        return record
    enrolled = passkeys.enrolled(sites_file)
    if not enrolled:
        raise PasskeyError('No passkey is saved. Run `brisk enroll` first')
    if len(enrolled) > 1:
        raise PasskeyError(f'Passkeys are saved for {", ".join(enrolled)}: choose one with --site')
    return passkeys.load(enrolled[0])


@contextmanager
def _one_at_a_time(path=None, poll=0.2):
    """Hold this computer's passkey sign-in lock (the one `login` takes), waiting for whoever has it: two feeds that find
    their sessions ended at 05:00 would otherwise open two Chromes at once. Released when the process ends, however it does."""
    path = Path(path) if path else config_dir() / 'passkey.lock'
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with open(path, 'a+b') as file:
        while True:
            try:
                if sys.platform == 'win32':
                    import msvcrt
                    file.seek(0)
                    msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                time.sleep(poll)
        try:
            yield
        finally:
            if sys.platform == 'win32':
                import msvcrt
                file.seek(0)
                msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)


def _check_user_id(user_id):
    if not isinstance(user_id, str) or not user_id.strip() or len(user_id) > 100 or any(c in user_id for c in '\r\n'):
        raise PasskeyError('The user ID must be one line of text')
    return user_id.strip()


def _request(site, login_url=None, launch_url=None, passkey_button=None, chrome=None, profile_dir=None, headless=None):
    request = {'loginUrl': login_url or site.login_url, 'launchUrl': launch_url or site.launch_url, 'cookieHost': site.cookie_host,
               'cookiePrefix': site.cookie_prefix, 'passkeyButton': passkey_button or site.passkey_button,
               'chrome': str(chrome) if chrome else None, 'profileDir': str(profile_dir) if profile_dir else None,
               'headless': headless}
    return {key: value for key, value in request.items() if value is not None}


def _send(proc, text):
    try:
        proc.stdin.write(text)
        proc.stdin.flush()
    except BrokenPipeError:
        pass  # The helper already stopped; its exit is reported below.


def _stop(proc):
    """Stop the helper. Closing its stdin makes it close Chrome and exit; a signal would not do on Windows, where
    terminating a process kills it outright and would leave Chrome and its temporary profile behind."""
    try:
        proc.stdin.close()
    except OSError:
        pass
    try:
        proc.wait(GRACE)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _run(mode, request, node, on_credential, on_registered=None):
    """Run the helper; its progress goes to our stderr, its credentials and result come back as JSON lines."""
    from briskapi import cli
    node = cli.runtime(node)
    cli.check_node(node)
    # The helper speaks UTF-8 (a passkey's user name may be Japanese); on Windows the default would be the code page.
    proc = subprocess.Popen([node, str(HELPER), mode], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding='utf-8')
    result = None
    try:
        _send(proc, json.dumps({'mode': mode, **request}) + '\n')
        for line in proc.stdout:
            try:
                message = json.loads(line)
            except ValueError:
                continue
            kind = message.get('type')
            if kind == 'credential':
                on_credential(message['credential'])
            elif kind == 'registered' and on_registered:
                on_registered()
                _send(proc, 'done\n')
            elif kind == 'result':
                result = message
        code = proc.wait()
    finally:
        if proc.poll() is None:
            _stop(proc)
    if result is None:
        raise PasskeyError(f'The passkey helper stopped unexpectedly (exit {code})')
    if not result.get('ok'):
        raise PasskeyError(result.get('error') or 'The passkey helper failed')
    return result


def _confirm():
    if not sys.stdin.isatty():
        raise PasskeyError('Enrolling needs an interactive terminal: it waits for you to finish the registration at the broker')
    print('Finish registering the passkey in the Chrome window. When the site says it is registered,\n'
          'press Enter here to save the passkey and close Chrome.', file=sys.stderr)
    input()


def _ask_user_id(site):
    if not sys.stdin.isatty():
        raise PasskeyError(f'{site.name}\'s passkey sign-in asks for your user ID: pass user_id= or enroll from a terminal')
    print(f'{site.name}\'s passkey sign-in asks for your user ID before the passkey. It is kept with the passkey and\n'
          'typed into the login page each time.', file=sys.stderr)
    return input('User ID: ')


def _choose(sites):
    if not sys.stdin.isatty():
        raise PasskeyError('Choose your broker with --site (see `brisk sites`)')
    ordered = list(sites.values())
    print('Which broker do you use BRiSK with?', file=sys.stderr)
    for number, site in enumerate(ordered, 1):
        print(f'  {number}) {site.name} ({site.id})', file=sys.stderr)
    answer = input('Number or id: ').strip().lower()
    if answer.isdigit() and 1 <= int(answer) <= len(ordered):
        return ordered[int(answer) - 1].id
    return answer


def _check_client(site):
    """A data client sends its cookies to one host. Cookies of any other site must never be handed to it."""
    if site.client:
        from briskapi import brisk
        served = brisk.SITES[site.client]
        host = urllib.parse.urlsplit(served.origin).hostname
        if site.cookie_host != host:
            raise PasskeyError(f'Cookies of {site.cookie_host} are never given to the {served.name} client, which only talks to {host}')


def _deliver(site, cookies, remember):
    if site.client:
        from briskapi import brisk
        client = brisk._login(site=site.client, cookies=cookies, remember=remember)
        return SignIn(site, cookies, client, brisk.cookies_path(site.client) if remember else None)
    path = None
    if remember:
        path = cookies_path(site.id)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(cookies, f)
    return SignIn(site, cookies, None, path)


def enroll(site=None, login_url=None, chrome=None, profile_dir=None, replace=False, store=None, node=None, confirm=None,
           headless=False, choose=None, sites_file=None, user_id=None):
    """Capture the passkey your broker registers in a Chrome you drive by hand, and save it as that broker's.

    Each broker keeps its own: enrolling another leaves the others alone; `replace` is for enrolling the same one again.
    A site whose passkey form wants a user ID first (e-shiten) also needs `user_id` (asked if omitted).
    Returns a public summary (never the private key)."""
    store = store or Passkeys()
    sites = load_sites(sites_file)
    chosen = get_site(sites, site or (choose or _choose)(sites))
    if store.load(chosen.id) and not replace:
        raise PasskeyError(f'A passkey for {chosen.id} is already saved. Use --replace to enroll it again (the old one '
                           'stays registered at the broker until you remove it in its security settings)')
    record = {'site': chosen.id}
    if chosen.user_field:
        record['user_id'] = _check_user_id(user_id if user_id is not None else _ask_user_id(chosen))
    latest = []
    _run('enroll', _request(chosen, login_url=login_url or chosen.enroll_url, chrome=chrome, profile_dir=profile_dir,
                            headless=headless or None), node, on_credential=latest.append, on_registered=confirm or _confirm)
    if not latest:
        raise PasskeyError('No passkey was captured')
    store.save(chosen.id, {**record, 'credential': latest[-1]})  # Only now: a failed enrollment must not replace a working one.
    return {'site': chosen.id, 'rp_id': latest[-1]['rpId'], 'user_name': latest[-1].get('userName'),
            'stored_in': store.description(chosen.id)}


def login(remember=True, login_url=None, launch_url=None, passkey_button=None, chrome=None, profile_dir=None, headless=False,
          store=None, node=None, sites_file=None, site=None) -> SignIn:
    """Sign in to a broker with its saved passkey and keep its BRiSK cookies. `site` says which when more than one broker
    has a passkey; with only one it is that one. Signing in takes this computer's passkey lock, so two sign-ins (two feeds,
    say) go one after the other."""
    store = store or Passkeys()
    record = _record(store, site, sites_file)
    site = get_site(load_sites(sites_file), record['site'])
    _check_client(site)
    request = {**_request(site, login_url, launch_url, passkey_button, chrome, profile_dir, headless or None),
               'credential': record['credential']}
    if site.user_field:
        if not record.get('user_id'):
            raise PasskeyError(f'{site.name}\'s passkey sign-in asks for your user ID, which was not saved; '
                               f'run `brisk enroll --site {site.id} --replace`')
        request.update(userField=site.user_field, userId=record['user_id'])
    kept = {key: record[key] for key in ('site', 'user_id') if key in record}
    with _one_at_a_time():
        result = _run('login', request, node, on_credential=lambda credential: store.save(site.id, {**kept, 'credential': credential}))
    return _deliver(site, result['cookies'], remember)


def forget(site=None, store=None, sites_file=None):
    """Delete a saved passkey (`site`'s, or the only one saved). It stays registered at the broker until you remove it in
    its settings. Returns the site it deleted, or None when there was none."""
    store = store or Passkeys()
    if site is None:
        enrolled = store.enrolled(sites_file)
        if len(enrolled) > 1:
            raise PasskeyError(f'Passkeys are saved for {", ".join(enrolled)}: choose one with --site')
        site = enrolled[0] if enrolled else None
    if site is not None:
        store.delete(site)
    return site
