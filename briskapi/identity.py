"""Your archive identity: a random secret made on this computer, never registered.

The archive files your uploads under a one-way hash of it, your owner ID, and lists,
downloads and deletes only that owner's. Whoever holds the secret is that owner.

So that it is hard to lose, the secret is kept twice: in the system's credential store
(the macOS Keychain or Windows Credential Manager, which survive a deleted config folder
and go into the system's backups) and in an owner-only file, `identity.json` beside the
upload choice. Each copy restores the other. It also reads as a short **recovery code**
(`brisk identity --recovery-code`), shown once when the identity is made: write it down, and
`brisk identity --recover CODE` brings your archive to any computer. BRISK_IDENTITY overrides
both copies; BRISK_IDENTITY_STORE=file keeps only the file.
"""
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sys

from briskapi.schema import owner, require

PREFIX = 'brisk-id-v1-'
SERVICE = 'briskapi-archive-identity'


def path() -> Path:
    return Path(os.environ.get('XDG_CONFIG_HOME') or Path.home() / '.config') / 'brisk' / 'identity.json'


class FileCopy:
    """The secret in an owner-only file."""
    description = 'file'

    def load(self):
        try:
            return json.loads(path().read_text())['secret']
        except FileNotFoundError:
            return None
        except (ValueError, KeyError, TypeError) as error:
            raise ValueError(f'Unreadable identity file {path()}: {error}') from None

    def save(self, value):
        target = path()
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.load()
        except ValueError:  # an unreadable file may still hold someone's identity: set it aside, never overwrite it
            os.replace(target, target.with_name(target.name + '.unreadable'))
        staging = target.with_name(target.name + '.tmp')
        staging.unlink(missing_ok=True)
        with os.fdopen(os.open(staging, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as f:
            json.dump({'secret': value, 'owner': owner(value),
                       'saved_at': dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}, f, indent=2)
            f.write('\n')
        os.replace(staging, target)


class SystemCopy:
    """The secret in the system's credential store, through briskapi.passkey's Keychain/Credential Manager code."""

    def __init__(self, store):
        self.store, self.description = store, store.description

    def load(self):
        record = self.store.load()
        return record.get('secret') if isinstance(record, dict) else None

    def save(self, value):
        self.store.save({'secret': value, 'owner': owner(value)})


def copies():
    """Where the secret is kept: the system's credential store where there is one, and the file."""
    kind = os.environ.get('BRISK_IDENTITY_STORE') or {'darwin': 'keychain', 'win32': 'wincred'}.get(sys.platform, 'file')
    if kind == 'keychain':
        from briskapi.passkey import KeychainStore
        return [SystemCopy(KeychainStore(service=SERVICE)), FileCopy()]
    if kind == 'wincred':
        from briskapi.passkey import CredentialManagerStore
        return [SystemCopy(CredentialManagerStore(target=SERVICE)), FileCopy()]
    return [FileCopy()]


def _read(copy):
    try:
        return copy.load()
    except Exception:  # noqa: BLE001 - one unreadable copy must not hide the other
        return None


def secret(create=True) -> str | None:
    """The identity secret: BRISK_IDENTITY, else the saved one (repairing a missing copy), else (with
    `create`) a new one, saved."""
    value = os.environ.get('BRISK_IDENTITY')
    if value:
        owner(value)
        return value
    found = [(copy, _read(copy)) for copy in copies()]
    values = {v for _, v in found if v}
    require(len(values) <= 1, f'The identity copies differ ({", ".join(c.description for c, v in found if v)}); '
                              'choose one with `brisk identity --recover CODE --replace`')
    if values:
        value = values.pop()
        owner(value)
        for copy, saved in found:
            if saved != value:
                _write(copy, value)
        return value
    _check_file()
    return save(new()) if create else None


def _check_file():
    if path().exists():
        FileCopy().load()  # raises for an unreadable file rather than silently replacing it


def _write(copy, value):
    try:
        copy.save(value)
        return True
    except Exception:  # noqa: BLE001 - the other copy still holds it
        return False


def new() -> str:
    return PREFIX + base64.b32encode(secrets.token_bytes(16)).decode().rstrip('=').lower()


def save(value: str, replace=False) -> str:
    """Keep `value` as this computer's identity, in every copy."""
    owner(value)
    current = {v for v in map(_read, copies()) if v}
    require(not current - {value} or replace,
            'This computer already has another identity; its uploads stay reachable only with it. '
            'Pass replace=True (--replace) to switch, after keeping its recovery code')
    saved = [copy.description for copy in copies() if _write(copy, value)]
    require(saved, 'Could not save the identity anywhere')
    return value


def _checksum(body: str) -> str:
    return base64.b32encode(hashlib.sha256(body.encode()).digest()).decode()[:2]


def recovery_code(value: str) -> str:
    """The secret as a code to write down: its 26 letters and digits plus 2 checksum characters, in groups."""
    owner(value)
    body = value[len(PREFIX):].upper()
    code = body + _checksum(body)
    return '-'.join(code[i:i + 4] for i in range(0, len(code), 4))


def from_recovery_code(code: str) -> str:
    """The secret a recovery code (or the secret itself) stands for; refuses a mistyped code."""
    if code.strip().lower().startswith(PREFIX):
        value = code.strip().lower()
        owner(value)
        return value
    # Read forgivingly: case, spaces and dashes don't matter, and 0, 1 and 8 are taken for O, I and B.
    text = re.sub(r'[\s-]', '', code.upper()).translate(str.maketrans('018', 'OIB'))
    require(re.fullmatch(r'[A-Z2-7]{28}', text) and _checksum(text[:26]) == text[26:],
            'That recovery code is mistyped (28 letters and digits, checked)')
    return PREFIX + text[:26].lower()


def owner_id() -> str:
    """This computer's owner ID (creating the identity if needed); safe to show."""
    return owner(secret())


def where() -> list[str]:
    """Where this computer keeps the identity."""
    return [str(path()) if isinstance(copy, FileCopy) else copy.description for copy in copies()]


def ensure() -> tuple[str, bool]:
    """(the secret, whether it was just made): the identity is made automatically when there is none."""
    existing = secret(create=False)
    return (existing, False) if existing else (secret(), True)
