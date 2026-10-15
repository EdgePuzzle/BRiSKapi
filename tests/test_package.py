"""Install-time behavior: the Node preflight, --version and the PyPI package description."""
import os
import re
import subprocess
import tomllib
from pathlib import Path

import pytest

import briskapi
import briskapi.cli as cli
from briskapi import _live, brisk

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.delenv('BRISK_UPLOAD', raising=False)
    monkeypatch.setattr(briskapi, '_current', None)


def fake_node(tmp_path, output, executable=True):
    if os.name == 'nt':
        script = tmp_path / ('fake-node.cmd' if executable else 'fake-node.txt')
        script.write_text(f'@echo {output}\n', encoding='utf-8')
        return str(script)
    script = tmp_path / 'fake-node'
    script.write_text(f'#!/bin/sh\necho {output}\n')
    script.chmod(0o755 if executable else 0o644)
    return str(script)


def test_check_node_accepts_supported_versions(tmp_path):
    for output in ('v22.0.0', 'v25.2.1', 'unrecognised output'):
        cli.check_node(fake_node(tmp_path, output))
    cli.check_node()  # The real node on PATH, as the other tests need.


def test_check_node_explains_how_to_fix_it(tmp_path):
    with pytest.raises(briskapi.BriskError, match=r'Node\.js was not found.*nodejs\.org'):
        cli.check_node(str(tmp_path / 'missing'))
    with pytest.raises(briskapi.BriskError, match=r'v20\.11\.0 is too old.*Node 22\+.*nodejs\.org'):
        cli.check_node(fake_node(tmp_path, 'v20.11.0'))
    with pytest.raises(briskapi.BriskError, match=r'Could not run.*nodejs\.org'):
        cli.check_node(fake_node(tmp_path, 'v22.0.0', executable=False))


def test_check_node_reports_a_hung_node(monkeypatch):
    def hang(*args, **kwargs):
        raise subprocess.TimeoutExpired('node', 30)

    monkeypatch.setattr(cli.subprocess, 'run', hang)
    with pytest.raises(briskapi.BriskError, match='Could not run'):
        cli.check_node()


def test_every_entry_point_checks_node_first(tmp_path):
    missing = str(tmp_path / 'missing')
    with pytest.raises(briskapi.BriskError, match='not found'):
        next(_live.stream(cache=tmp_path, node=missing))
    with pytest.raises(briskapi.BriskError, match='not found'):
        briskapi.Feed(cache=tmp_path, node=missing)
    with pytest.raises(briskapi.BriskError, match='not found'):
        briskapi.connect(site='demo', cache=tmp_path, node=missing)
    with pytest.raises(briskapi.BriskError, match='not found'):
        cli.record_events(tmp_path / 'events.jsonl', cache=tmp_path, node=missing)
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    with pytest.raises(briskapi.BriskError, match='not found'):
        briskapi.connect(site='sbi', node=missing)
    assert briskapi._current is None


def test_cli_reports_errors_without_a_traceback(tmp_path, monkeypatch):
    monkeypatch.setenv('PATH', str(tmp_path))
    with pytest.raises(SystemExit) as stopped:
        cli.main(['live', '--site', 'demo', '--cache', str(tmp_path)])
    assert str(stopped.value.code).startswith('brisk: error: Node.js was not found')


def test_cli_version(capsys):
    with pytest.raises(SystemExit) as stopped:
        cli.main(['--version'])
    assert stopped.value.code == 0
    assert capsys.readouterr().out.strip() == f'brisk {briskapi.__version__}'


def test_version_matches_pyproject():
    project = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))['project']
    assert project['version'] == briskapi.__version__


def test_readme_links_work_on_pypi():
    # README.md is the PyPI description, where relative links do not resolve.
    readme = (ROOT / 'README.md').read_text(encoding='utf-8')
    assert not re.findall(r'\]\((?!https?://|#|mailto:)[^)]+\)', readme)
