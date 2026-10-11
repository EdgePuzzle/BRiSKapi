"""Check the 18000 master record layout on SBI's decoder of March 2026, offline and without an account.

Downloads pinned copies from pybrisk's research (https://github.com/obichan117/pybrisk): SBI's decoder
and, from pybrisk's capture of sbi.brisk.jp, only the master file it served. The capture also holds
pybrisk's own (expired) session, so the master is taken out of it in memory and nothing else is kept.
None of this belongs in Git: the cache is yours. Then runs briskapi's Decoder on them under Node's
permission model (reads only, no child processes) through sbi_master_layout.cjs.

    python tools/brisk_mock/sbi_master_layout.py --cache /tmp/sbi-master-cache
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
PYBRISK = 'https://raw.githubusercontent.com/obichan117/pybrisk/ca4c77d1553dfd4debdad3de41dbe77ef66dc104/research/'
CAPTURE = (PYBRISK + 'sbi.brisk.jp.har', '59cdf8088b7c0eeac0902d88dbb03f1657fb997667aa2c72ad7df92e0f2adc74')
DECODER = {
    'fita.wasm': (PYBRISK + 'ws_frames/fita.wasm', '5ee4f6010b825924da53aeb878616fc04370ff6ae875a031f82128c3ddcd6572'),
    'fita.js': (PYBRISK + 'ws_frames/fita_wasm.js', '7e0bec622c2526bbd7c2ee91e3e07452be04effcf0264a67aef5712ae6748b24'),
}
# sbi.brisk.jp names a master by its SHA-256: /api/master/<hash>.
MASTER = 'f7b029c68758670afe9ac226db629fceab7e56076dcbe703e6d6891234945a72'


def fetch(url: str, sha256: str) -> bytes:
    with urlopen(url, timeout=120) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != sha256:
        raise ValueError(f'{url} changed; re-audit before changing the pin')
    return data


def master_from_capture(capture: bytes) -> bytes:
    for entry in json.loads(capture)['log']['entries']:
        if entry['request']['url'].split('?')[0].endswith('/api/master/' + MASTER):
            data = base64.b64decode(entry['response']['content'].get('text') or '')
            if hashlib.sha256(data).hexdigest() == MASTER:
                return data
    raise ValueError('The capture holds no master file with the pinned hash')


def download(cache: Path) -> None:
    cache.mkdir(parents=True, exist_ok=True)
    wanted = {**{name: sha for name, (_, sha) in DECODER.items()}, 'master.dat': MASTER}
    for name, sha256 in wanted.items():
        dest = cache / name
        if dest.exists() and hashlib.sha256(dest.read_bytes()).hexdigest() == sha256:
            continue
        data = master_from_capture(fetch(*CAPTURE)) if name == 'master.dat' else fetch(*DECODER[name])
        temp = dest.with_suffix(dest.suffix + '.tmp')
        temp.write_bytes(data)
        temp.replace(dest)
        print(f'{name}: {len(data):,} bytes', file=sys.stderr)


def check(cache: Path, node: str = 'node') -> int:
    script = Path(__file__).with_name('sbi_master_layout.cjs')
    return subprocess.run([node, '--permission', f'--allow-fs-read={cache.resolve()}', f'--allow-fs-read={ROOT / "briskapi"}',
                           f'--allow-fs-read={script}', str(script), str(cache.resolve())], check=False).returncode


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--cache', required=True, type=Path)
    parser.add_argument('--node', default='node')
    args = parser.parse_args()
    download(args.cache.expanduser())
    sys.exit(check(args.cache.expanduser(), args.node))
