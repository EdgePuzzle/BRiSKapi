"""Install and exercise the wheel outside the checkout on POSIX or Windows."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import sysconfig
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache', type=Path, required=True)
    args = parser.parse_args()
    cache = args.cache.resolve()
    root = Path(__file__).resolve().parent.parent
    with tempfile.TemporaryDirectory(prefix='brisk-wheel-') as temporary:
        work = Path(temporary)
        subprocess.run([sys.executable, '-m', 'pip', 'wheel', '--no-deps', '-w', str(work / 'dist'), str(root)],
                       cwd=work, check=True)
        # With brisk-parquet inside, tagged for this computer, as the release's wheels have it.
        program = root / 'rust' / 'brisk_parquet' / 'target' / 'release' / ('brisk-parquet.exe' if sys.platform == 'win32' else 'brisk-parquet')
        if not program.exists():
            subprocess.run(['cargo', 'build', '--release', '--locked', '--manifest-path', str(root / 'rust' / 'brisk_parquet' / 'Cargo.toml')],
                           check=True)
        platform = sysconfig.get_platform().replace('-', '_').replace('.', '_')
        subprocess.run([sys.executable, str(root / 'tools' / 'platform_wheel.py'), str(next((work / 'dist').glob('briskapi-*-py3-none-any.whl'))),
                        str(program), platform, '--out', str(work / 'wheelhouse')], cwd=work, check=True)
        subprocess.run([sys.executable, '-m', 'venv', str(work / 'venv')], cwd=work, check=True)
        scripts = work / 'venv' / ('Scripts' if sys.platform == 'win32' else 'bin')
        python = scripts / ('python.exe' if sys.platform == 'win32' else 'python')
        subprocess.run([str(python), '-m', 'pip', 'install', str(next((work / 'wheelhouse').glob('briskapi-*.whl')))],
                       cwd=work, check=True)
        subprocess.run([str(python), '-I', '-c',
                        "import briskapi.cli as c, briskapi.schema as s; "
                        "assert c.DECODER.exists() and 'historical_mock' in s.REFERENCES"], cwd=work, check=True)
        subprocess.run([str(python), '-I', '-c', "import tempfile; from briskapi import _native; _native.run(['schemas', tempfile.mkdtemp()])"],
                       cwd=work, check=True)
        brisk = scripts / ('brisk.exe' if sys.platform == 'win32' else 'brisk')
        result = subprocess.run([str(brisk), 'live', '--site', 'demo', '--cache', str(cache), '--codes', '7203',
                                 '--speed', '0', '--limit-frames', '3', '--no-upload', '--quotes'],
                                cwd=work, check=True, stdout=subprocess.PIPE,
                                env={**os.environ, 'XDG_CONFIG_HOME': str(work / 'config'),
                                     'XDG_DATA_HOME': str(work / 'data'), 'XDG_CACHE_HOME': str(work / 'cache'),
                                     'BRISK_IDENTITY_STORE': 'file', 'BRISK_UPLOAD': '0'})
        rows = [json.loads(line) for line in result.stdout.splitlines()]
        assert rows and all(row['code'] == '7203' for row in rows)
        print(f'Installed wheel replayed {len(rows)} quotes outside the checkout')


if __name__ == '__main__':
    main()
