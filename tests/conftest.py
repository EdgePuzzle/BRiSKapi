import urllib.parse
import urllib.request

import pytest

import briskapi.cli as cli


@pytest.fixture(autouse=True)
def identity_in_a_file(monkeypatch):
    """Tests never touch the real Keychain or Credential Manager."""
    monkeypatch.setenv('BRISK_IDENTITY_STORE', 'file')


@pytest.fixture(autouse=True)
def data_in_a_temporary_folder(monkeypatch, tmp_path_factory):
    """Spools and kept recordings (briskapi._spool) never reach the real ~/.local/share/brisk."""
    monkeypatch.setenv('XDG_DATA_HOME', str(tmp_path_factory.mktemp('data')))


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    """No test reaches the archive (or any other remote host); local test servers stay reachable."""
    reach = urllib.request.urlopen

    def local_only(request, *args, **kwargs):
        url = request.full_url if isinstance(request, urllib.request.Request) else request
        if urllib.parse.urlsplit(url).hostname not in {'127.0.0.1', 'localhost', '::1'}:
            raise RuntimeError(f'Tests must not reach the network ({url}); patch urlopen or the archive call')
        return reach(request, *args, **kwargs)
    monkeypatch.setattr(urllib.request, 'urlopen', local_only)


@pytest.fixture(autouse=True)
def untold(monkeypatch):
    """Each test starts in a fresh process's state: the upload-by-default notice not yet shown."""
    monkeypatch.setattr(cli, '_told', False)
