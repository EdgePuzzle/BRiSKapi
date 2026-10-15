"""Make a platform wheel of briskapi: its pure wheel with brisk-parquet (rust/brisk_parquet) inside, tagged for one platform.

    python tools/platform_wheel.py dist/briskapi-X-py3-none-any.whl target/release/brisk-parquet manylinux2014_x86_64 --out dist

briskapi itself is pure Python; brisk-parquet is a program it runs (briskapi/_native.py), found next to the package, so a
wheel for a platform holds the one built for it, in briskapi/_bin/. It does not depend on the Python version: the tag is py3-none.
"""
import argparse
import base64
import csv
import hashlib
import io
from pathlib import Path
import zipfile


def digest(data: bytes) -> str:
    return 'sha256=' + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()


def platform_wheel(wheel: Path, program: Path, platform: str, out: Path) -> Path:
    """The wheel `wheel` (a pure wheel) with `program` in briskapi/_bin/, tagged `py3-none-PLATFORM`, written to `out`."""
    name = wheel.name
    if not name.endswith('-py3-none-any.whl'):
        raise ValueError(f'{name} is not a pure wheel (py3-none-any)')
    target = out / (name[:-len('any.whl')] + f'{platform}.whl')
    exe = '.exe' if platform.startswith('win') else ''
    entries = {}   # path -> (bytes, mode)
    with zipfile.ZipFile(wheel) as source:
        for info in source.infolist():
            if not info.is_dir():
                entries[info.filename] = (source.read(info), info.external_attr >> 16 or 0o644)
    dist_info = next(p.split('/')[0] for p in entries if p.split('/')[0].endswith('.dist-info'))
    entries[f'briskapi/_bin/brisk-parquet{exe}'] = (program.read_bytes(), 0o755)
    wheel_file, _ = entries[f'{dist_info}/WHEEL']
    lines = []
    for line in wheel_file.decode().splitlines():
        if line.startswith('Root-Is-Purelib:'):
            line = 'Root-Is-Purelib: false'
        elif line.startswith('Tag:'):
            line = f'Tag: py3-none-{platform}'
        lines.append(line)
    entries[f'{dist_info}/WHEEL'] = (('\n'.join(lines) + '\n').encode(), 0o644)
    record = io.StringIO()
    writer = csv.writer(record, lineterminator='\n')
    for path, (data, _) in sorted(entries.items()):
        if path != f'{dist_info}/RECORD':
            writer.writerow([path, digest(data), len(data)])
    writer.writerow([f'{dist_info}/RECORD', '', ''])
    entries[f'{dist_info}/RECORD'] = (record.getvalue().encode(), 0o644)
    out.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as built:
        for path, (data, mode) in entries.items():
            info = zipfile.ZipInfo(path, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type, info.external_attr = zipfile.ZIP_DEFLATED, mode << 16
            built.writestr(info, data)
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('wheel', type=Path)
    parser.add_argument('program', type=Path)
    parser.add_argument('platform', help='the wheel platform tag, e.g. manylinux2014_x86_64, macosx_11_0_arm64, win_amd64')
    parser.add_argument('--out', type=Path, default=Path('dist'))
    args = parser.parse_args()
    print(platform_wheel(args.wheel, args.program, args.platform, args.out))


if __name__ == '__main__':
    main()
