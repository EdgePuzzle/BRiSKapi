"""Run the API reference's examples and keep what they print, for the site's response cards.

    .venv/bin/python docs/site/capture.py --demo DIR [--only SLUG ...]

Cloud examples query the public demo dataset (XTKS.BRISKDEMO) with a throwaway identity made here and never
saved. Local examples read a demo recording at DIR (made by `brisk record --web --no-upload`), linked as
recordings/demo. Live-feed examples replay the public BRiSK Next demo from BRiSK's website. Nothing is uploaded:
BRISK_UPLOAD=0 and a fresh, temporary configuration. Entries marked illustrative keep their written responses.
Writes docs/site/responses.json.
"""
from __future__ import annotations

import argparse
import ast
import datetime as dt
import json
import os
from pathlib import Path
import pprint
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path[:0] = [str(HERE), str(ROOT)]

import reference  # noqa: E402
from briskapi import __version__, identity  # noqa: E402

OUT = HERE / 'responses.json'
# Not shown on the page: wide enough tables that pandas does not wrap them.
PRELUDE = ('try:\n    import pandas\n    pandas.set_option("display.width", 110)\n    pandas.set_option("display.max_columns", 12)\n'
           'except ImportError:\n    pass\n')
MAX_LINES = 40


def run(language, code, work, env):
    """Run one example in a fresh folder holding recordings/demo; returns what it printed."""
    (work / 'recordings').mkdir(parents=True)
    (work / 'recordings' / 'demo').symlink_to(env['DEMO'])
    if language == 'Python':
        command = [sys.executable, '-c', PRELUDE + code]
    else:
        command = ['bash', '-c', code.replace('{QUERY_URL}', env['QUERY_URL'])]
    result = subprocess.run(command, cwd=work, env=env, capture_output=True, text=True, timeout=1800)
    if result.returncode:
        raise SystemExit(f'{language} example failed:\n{code}\n{result.stderr[-2000:]}')
    return result.stdout


def shorten(value):
    """Long lists of plain values cut to their first few, so that a manifest's 4,000 codes do not fill the card."""
    if isinstance(value, dict):
        return {k: shorten(v) for k, v in value.items()}
    if isinstance(value, list):
        if len(value) > 8 and all(isinstance(v, (str, int, float)) for v in value):
            return value[:5] + [f'… {len(value) - 5} more']
        return [shorten(v) for v in value]
    return value


def tidy(text, work, key):
    text = text.replace(str(work), '.').rstrip()
    assert key not in text and str(Path.home()) not in text, 'the output names the key or this computer'
    lines = text.splitlines()
    if len(lines) == 1:
        try:   # one long JSON or Python value: spread over lines, as Databento's examples show them
            text = json.dumps(shorten(json.loads(text)), indent=2, ensure_ascii=False)
        except ValueError:
            try:
                value = ast.literal_eval(text)
                if isinstance(value, (list, dict)) and len(text) > 100:
                    text = pprint.pformat(value, width=100, sort_dicts=False)
            except (ValueError, SyntaxError):
                pass
        lines = text.splitlines()
    if len(lines) > MAX_LINES:
        lines = lines[:MAX_LINES] + ['…']
    return '\n'.join(line.rstrip() for line in lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--demo', type=Path, required=True, help='A demo recording (brisk record --web --no-upload)')
    parser.add_argument('--only', nargs='*', help='Capture only these method slugs, keeping the rest')
    args = parser.parse_args()
    responses = json.loads(OUT.read_text()) if OUT.exists() else {}
    key = identity.new()
    settings = json.loads((ROOT / 'briskapi' / 'archive.json').read_text())
    with tempfile.TemporaryDirectory() as tmp:
        env = {k: v for k, v in os.environ.items() if not k.startswith(('AWS_', 'BRISK_'))}
        env |= {'BRISK_IDENTITY': key, 'BRISK_IDENTITY_STORE': 'file', 'BRISK_UPLOAD': '0',
                'XDG_CONFIG_HOME': f'{tmp}/config', 'XDG_CACHE_HOME': f'{tmp}/cache', 'PYTHONPATH': str(ROOT),
                'PATH': f'{Path(sys.executable).parent}:{env["PATH"]}', 'QUERY_URL': settings['query_url'],
                'DEMO': str(args.demo.resolve())}
        for _, methods in reference.REFERENCE:
            for method in methods:
                if method.illustrative or (args.only and method.slug not in args.only):
                    continue
                print(method.slug, file=sys.stderr)
                responses[method.slug] = {}
                for language, code in method.examples.items():
                    work = Path(tmp) / f'{method.slug.replace("/", "-")}-{language}'
                    responses[method.slug][language] = tidy(run(language, code, work, env), work, key)
    responses['_captured'] = {'date': dt.date.today().isoformat(), 'briskapi': __version__}
    OUT.write_text(json.dumps(dict(sorted(responses.items())), indent=1, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    main()
