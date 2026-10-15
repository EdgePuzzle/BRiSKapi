"""tools/platform_wheel.py: briskapi's pure wheel with brisk-parquet inside, tagged for a platform."""
import base64
import csv
import hashlib
import importlib.util
import io
from pathlib import Path
import zipfile

import pytest

spec = importlib.util.spec_from_file_location('platform_wheel', Path(__file__).resolve().parents[1] / 'tools/platform_wheel.py')
platform_wheel = importlib.util.module_from_spec(spec)
spec.loader.exec_module(platform_wheel)


def pure_wheel(tmp_path):
    files = {'briskapi/__init__.py': b'', 'briskapi/cli.py': b'print(1)\n',
             'briskapi-9.9.dist-info/METADATA': b'Name: briskapi\nVersion: 9.9\n',
             'briskapi-9.9.dist-info/WHEEL': b'Wheel-Version: 1.0\nGenerator: setuptools\nRoot-Is-Purelib: true\nTag: py3-none-any\n'}
    record = io.StringIO()
    writer = csv.writer(record, lineterminator='\n')
    for path, data in files.items():
        writer.writerow([path, platform_wheel.digest(data), len(data)])
    writer.writerow(['briskapi-9.9.dist-info/RECORD', '', ''])
    path = tmp_path / 'briskapi-9.9-py3-none-any.whl'
    with zipfile.ZipFile(path, 'w') as z:
        for name, data in files.items():
            z.writestr(name, data)
        z.writestr('briskapi-9.9.dist-info/RECORD', record.getvalue())
    return path


@pytest.mark.parametrize('platform, name', [('manylinux2014_x86_64', 'brisk-parquet'), ('win_amd64', 'brisk-parquet.exe')])
def test_the_program_goes_into_the_wheel_which_is_tagged_for_its_platform(tmp_path, platform, name):
    program = tmp_path / 'built'
    program.write_bytes(b'\x7fELF program')
    built = platform_wheel.platform_wheel(pure_wheel(tmp_path), program, platform, tmp_path / 'dist')
    assert built.name == f'briskapi-9.9-py3-none-{platform}.whl'
    with zipfile.ZipFile(built) as z:
        info = z.getinfo(f'briskapi/_bin/{name}')
        assert z.read(info) == b'\x7fELF program' and info.external_attr >> 16 == 0o755
        wheel = z.read('briskapi-9.9.dist-info/WHEEL').decode()
        assert f'Tag: py3-none-{platform}' in wheel and 'Root-Is-Purelib: false' in wheel and 'py3-none-any' not in wheel
        # RECORD names every file with its hash and size.
        rows = {r[0]: r for r in csv.reader(io.StringIO(z.read('briskapi-9.9.dist-info/RECORD').decode()))}
        assert set(rows) == set(z.namelist())
        for path, (_, hash_, size) in ((p, rows[p]) for p in rows if p != 'briskapi-9.9.dist-info/RECORD'):
            data = z.read(path)
            assert hash_ == 'sha256=' + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()
            assert int(size) == len(data)


def test_only_a_pure_wheel_is_made_into_a_platform_wheel(tmp_path):
    path = tmp_path / 'briskapi-9.9-py3-none-manylinux2014_x86_64.whl'
    path.write_bytes(b'')
    with pytest.raises(ValueError, match='not a pure wheel'):
        platform_wheel.platform_wheel(path, tmp_path / 'x', 'win_amd64', tmp_path)
