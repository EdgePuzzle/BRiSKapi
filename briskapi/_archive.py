"""Your private archive: list, pull (verified and cached), upload and delete your own recordings."""
from __future__ import annotations

import os
from pathlib import Path

from briskapi import cli, identity
from briskapi._recording import Recording, Table
from briskapi.schema import RECORDING_PREFIX


def cache_dir() -> Path:
    return Path(os.environ.get('XDG_CACHE_HOME') or Path.home() / '.cache') / 'brisk'


class Archive:
    """Your recordings in the Tokyo archive, reachable with this computer's identity only.

        archive = Archive()
        rows = archive.recordings(source="historical_mock")
        rec = archive.pull(rows[0]["prefix"])
    """

    def __init__(self, config=None):
        self.config = cli.settings(Path(config) if config else None)

    @property
    def owner(self) -> str:
        """Your owner ID: the archive's name for you, a one-way hash of your identity secret."""
        return identity.owner_id()

    def recordings(self, date=None, source=None) -> Table:
        """Your recordings, optionally filtered by YYYYMMDD trading date and source."""
        rows = []
        for prefix, m in cli.manifests(self.config['api_url'], date):
            s = m['summary']
            if source is None or s['source'] == source:
                rows.append({'prefix': prefix, 'source': s['source'], 'trading_date': s['trading_date'],
                             'securities': len(s['codes']), 'batches': s['batches'],
                             'quote_updates': s['quote_updates'], 'bytes': m['bytes'], 'sha256': m['sha256']})
        return Table(rows)

    def timing(self, date=None) -> Table:
        """Your timing-only reports (from broker BRiSK sessions), optionally for one YYYYMMDD date."""
        return Table({'key': key, **report} for key, report in cli.timing_reports(self.config['api_url'], date))

    def pull(self, prefix, output=None) -> Recording:
        """Download, verify and decode one of your recordings. Without `output`, downloads are cached."""
        if not RECORDING_PREFIX.fullmatch(prefix):
            raise ValueError('Invalid archive prefix')
        target = Path(output) if output else cache_dir() / prefix
        # Cache entries only appear after verification (pull moves them atomically).
        if output or not target.exists():
            cli.pull(self.config['api_url'], prefix, target)
        return Recording(target)

    def upload(self, directory, timeout=660) -> dict:
        """Upload a prepared package and wait for its automatic validation."""
        return cli.upload(Path(directory), self.config['api_url'], timeout)

    def delete(self, prefix) -> dict:
        """Remove one of your recordings (or timing reports) from your archive; restore() brings it back within
        30 days; after that it is removed."""
        return cli.delete(self.config['api_url'], prefix)

    def deleted(self, date=None) -> Table:
        """What you deleted and can still restore, with deleted_at and restore_until."""
        return Table(cli.deleted(self.config['api_url'], date))

    def restore(self, prefix) -> dict:
        """Bring back one of your recordings (or timing reports) deleted in the last 30 days."""
        return cli.restore(self.config['api_url'], prefix)

    def prune(self, directory) -> dict:
        """Free disk space: remove a local package your archive holds (verified by SHA-256); pull brings it back."""
        return cli.prune(self.config['api_url'], Path(directory))
