"""Build the documentation site (GitHub Pages) from the repository's documents and the API reference.

    python docs/site/build.py            # writes build/site/html
    python docs/site/build.py --serve    # and previews it at http://127.0.0.1:8000

README.md and README.ja.md become guide pages, one per section; the other documents become pages as they
are, their Japanese translations the Japanese site's; reference.py and responses.json (capture.py) become
the API reference. Links to files that are not pages go to GitHub. Needs docs/site/requirements.txt.
"""
from __future__ import annotations

import argparse
import html
import json
import posixpath
import re
import shutil
import subprocess
import sys
from pathlib import Path

from pymdownx.slugs import slugify

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))

import reference  # noqa: E402

OUT = ROOT / 'build' / 'site'
DOCS = OUT / 'docs'
SITE = 'https://honvl.github.io/BRiSKapi/'
BLOB = 'https://github.com/honvl/BRiSKapi/blob/main/'
SLUG = slugify(case='lower')

# README sections in order (README.ja.md has the same ones): page, title, Japanese title. None: not a page.
SECTIONS = [
    ('guides/which-api.md', 'Which API should I use?', 'どの API を使うか'),
    ('guides/install.md', 'Install', 'インストール'),
    ('guides/recording.md', 'Recording', '記録する'),
    ('guides/live-feed.md', 'Live feed', 'ライブフィード'),
    ('guides/recordings.md', 'Recordings', '記録データ'),
    ('guides/broker.md', 'Your broker\'s BRiSK', '証券会社の BRiSK'),
    ('guides/passkey.md', 'Passkey sign-in', 'パスキーでのサインイン'),
    ('guides/api-overview.md', 'API overview', 'API 一覧'),
    ('guides/cli.md', 'Command line', 'コマンドライン'),
    ('guides/archive.md', 'Your private archive', '自分専用のアーカイブ'),
    ('guides/cloud.md', 'Querying in the cloud', 'クラウドでの照会'),
    None,   # More documentation: the site's navigation
]
# Whole documents: repository path, page, title, Japanese title. A translation X.ja.md becomes the page's Japanese version.
DOCUMENTS = [
    ('docs/data-format.md', 'data-format.md', 'Data format', 'データ形式'),
    ('PRIVACY.md', 'privacy.md', 'Privacy policy', 'プライバシーポリシー'),
    ('ARCHITECTURE.md', 'internals/architecture.md', 'How it works', '仕組み'),
    ('docs/cloud-api.md', 'internals/cloud-api.md', 'Cloud API design', 'クラウド API の設計'),
    ('infra/README.md', 'internals/deploying.md', 'Deploying an archive', 'アーカイブのデプロイ'),
    ('tools/brisk_mock/README.md', 'internals/demo-collector.md', 'Demo collector and timing', 'デモのコレクターとタイミング'),
    ('CONTRIBUTING.md', 'contributing.md', 'Contributing', '開発への参加'),
    ('THIRD_PARTY.md', 'third-party.md', 'Third-party notices', '第三者の権利表示'),
]
GROUPS = {   # reference groups: slug prefix, title, Japanese title
    'historical/client': ('Client', 'クライアント'), 'historical/timeseries/': ('Timeseries', 'Timeseries'),
    'historical/metadata/': ('Metadata', 'Metadata'), 'historical/store/': ('DataStore', 'DataStore'),
    'python/ticker_': ('Ticker', 'Ticker'), 'python/market_': ('Market', 'Market'), 'python/feed_': ('Feed', 'Feed'),
    'python/archive_': ('Archive', 'Archive'), 'python/brisk_': ('Your broker', '証券会社'), 'python/': ('Functions', '関数'),
}
LANGUAGES = {'Python': 'python', 'HTTP': 'bash', 'CLI': 'bash'}
LANGUAGE_LINE = re.compile(r'^\[English\]\([^)]*\) \| \[日本語\]\([^)]*\).*$', re.M)
LINK = re.compile(r'(\]\()([^)\s]+)(\))')
TRANSLATIONS = {'Home': 'ホーム', 'Getting started': 'はじめに', 'Your archive': 'アーカイブ', 'Reference': 'リファレンス',
                'Cloud API': 'クラウド API', 'Python API': 'Python API', 'About': 'このプロジェクトについて'}


def lines_outside_code(text):
    """(line, in_code) for each line; a fence's own lines count as code."""
    fence = None
    for line in text.splitlines():
        marker = re.match(r'\s*(```|~~~)', line)
        if marker and (fence is None or marker[1] == fence):
            fence = None if fence else marker[1]
            yield line, True
        else:
            yield line, fence is not None


def split(text):
    """The README's intro and its H2 sections: [(heading, body lines)]."""
    parts = [('', [])]
    for line, code in lines_outside_code(text):
        if not code and line.startswith('## '):
            parts.append((line[3:].strip(), []))
        else:
            parts[-1][1].append(line)
    return parts


def anchors(lines):
    return [SLUG(re.sub(r'[`*]', '', line.lstrip('#').strip()), '-') for line, code in lines_outside_code('\n'.join(lines))
            if not code and re.match(r'#{1,6} ', line)]


class Site:
    def __init__(self):
        self.pages = {}         # site page (Japanese: .ja.md) -> markdown
        self.files = {}         # repository document -> site page (without the language suffix)
        self.translated = set()  # English documents with a Japanese translation
        self.readme = {}        # (README file, anchor) -> page#anchor

    def page_url(self, page, japanese=False):
        path = page[:-len('.md')]
        path = '' if path == 'index' else path.removesuffix('/index') + '/'
        return SITE + ('ja/' if japanese else '') + path

    def rewrite(self, text, source, page):
        """Point a document's links at the site's pages, or at GitHub for files that are not pages."""
        japanese = source.endswith('.ja.md')

        def fix(match):
            url = match[2]
            if url.startswith('/'):   # a site page, written from the site's root
                return match[1] + root_link(url, page) + match[3]
            if url.startswith(BLOB):
                target = url[len(BLOB):]
            elif re.match(r'[a-z]+:', url):
                return match[0]
            elif url.startswith('#'):
                target = source + url
            else:
                target = posixpath.normpath(posixpath.join(posixpath.dirname(source), url))
            path, _, anchor = target.partition('#')
            if path in ('README.md', 'README.ja.md'):
                dest = self.readme.get((path, anchor)) or 'index.md' + (f'#{anchor}' if anchor else '')
            elif path in self.files:
                dest = self.files[path] + (f'#{anchor}' if anchor else '')
            else:
                return match[1] + BLOB + target + match[3]
            if japanese and path in self.translated:   # the English original, named on purpose
                return match[1] + self.page_url(self.files[path]) + (f'#{anchor}' if anchor else '') + match[3]
            if not japanese and path.endswith('.ja.md'):
                return match[1] + self.page_url(self.files[path], True) + (f'#{anchor}' if anchor else '') + match[3]
            dest_page, _, anchor = dest.partition('#')
            if dest_page == page and anchor:
                return match[1] + f'#{anchor}' + match[3]
            relative = posixpath.relpath(dest_page, posixpath.dirname(page) or '.')
            return match[1] + relative + (f'#{anchor}' if anchor else '') + match[3]

        return LINK.sub(fix, text)

    def add(self, page, text, source, japanese=False):
        meta, text = re.match(r'(---\n.*?\n---\n)?(.*)', LANGUAGE_LINE.sub('', text).lstrip('\n'), re.S).groups()
        text = markdown_lists(text)
        text = (meta or '') + (join_japanese(text) if japanese else text)
        self.pages[page.replace('.md', '.ja.md') if japanese else page] = self.rewrite(text, source, page)

    def readmes(self):
        for name, japanese in (('README.md', False), ('README.ja.md', True)):
            parts = split((ROOT / name).read_text())
            assert len(parts) == len(SECTIONS) + 1, f'{name}: its sections changed; update SECTIONS'
            for (heading, body), section in zip(parts[1:], SECTIONS):
                if section:
                    self.readme[(name, SLUG(re.sub(r'[`*]', '', heading), '-'))] = section[0]
                    for anchor in anchors(body):
                        self.readme[(name, anchor)] = f'{section[0]}#{anchor}'
        for name, japanese in (('README.md', False), ('README.ja.md', True)):
            parts = split((ROOT / name).read_text())
            intro = '\n'.join(parts[0][1])
            self.add('index.md', home(intro, japanese), name, japanese)
            for (heading, body), section in zip(parts[1:], SECTIONS):
                if not section:
                    continue
                body = [line[1:] if not code and re.match(r'#{3,6} ', line) else line
                        for line, code in lines_outside_code('\n'.join(body))]
                text = f'# {heading}\n\n' + '\n'.join(body).strip() + '\n'
                if section[0] == 'guides/api-overview.md':   # the README's "API reference": here, beside the reference
                    text = link_calls(text.replace(heading, section[2] if japanese else section[1], 1))
                self.add(section[0], text, name, japanese)

    def documents(self):
        for source, page, *_ in DOCUMENTS:
            self.files[source] = page
            translation = source.replace('.md', '.ja.md')
            if (ROOT / translation).exists():
                self.files[translation] = page
                self.translated.add(source)
        self.files['README.md'] = self.files['README.ja.md'] = 'index.md'
        for source, page, title, title_ja in DOCUMENTS:
            for path, japanese in ((source, False), (source.replace('.md', '.ja.md'), True)):
                if (ROOT / path).exists():
                    text = (ROOT / path).read_text()
                    if not text.startswith('# '):
                        text = f'# {title_ja if japanese else title}\n\n' + text
                    if japanese:
                        text = english_anchors(text, (ROOT / source).read_text())
                    self.add(page, text, path, japanese)


def markdown_lists(text):
    """GitHub's lists in Python-Markdown's terms: a blank line before a list that follows a paragraph, and nested
    items indented by four spaces rather than two or three."""
    out, previous = [], ''
    item = re.compile(r'(\s*(?:>\s*)?)(?:[-*]|\d+\.) ')
    for line, code in lines_outside_code(text):
        if not code:
            nested = re.match(r'( {1,7})((?:[-*]|\d+\.) .*)', line)
            if nested and len(nested[1]) % 4:
                line = ' ' * (4 * -(-len(nested[1]) // 4)) + nested[2]
            starts = item.match(line)
            continued = re.match(r'(?:>\s?)?\s{2,}\S', previous)   # an item's own continuation line
            if starts and previous.strip() not in ('', '>') and not item.match(previous) and not continued:
                out.append(starts[1].rstrip())
        out.append(line)
        previous = line if not code else ''
    return '\n'.join(out) + '\n'


CJK = re.compile(r'[\u3000-\u30ff\u3400-\u9fff\uff00-\uffef]')
BLOCK = re.compile(r'\s*(#|\||>|[-*] |\d+\. |<|```|~~~|---|===|$)')


def join_japanese(text):
    """Japanese wrapped across source lines, joined: a browser shows each line break in a paragraph as a space,
    which Japanese doesn't put between words."""
    out = []
    for line, code in lines_outside_code(text):
        previous = out[-1] if out else ''
        if (not code and out and not BLOCK.match(line) and not BLOCK.match(previous) or
                not code and out and line.startswith('  ') and not BLOCK.match(line.lstrip()) and previous.strip()) and \
                previous.strip() and not previous.startswith(('    ', '\t')) and \
                CJK.match(previous.rstrip()[-1]) and CJK.match(line.lstrip()[0]):
            out[-1] = previous.rstrip() + line.lstrip()
        else:
            out.append(line)
    return '\n'.join(out) + '\n'


def english_anchors(translation, original):
    """The translation with the original's heading anchors too, so that a link to #local-and-cloud-names also
    works on the Japanese page (the English reference pages link there from the Japanese site)."""
    english = anchors(original.splitlines())
    if len(anchors(translation.splitlines())) != len(english):
        raise SystemExit('A translation\'s headings no longer match the original\'s')
    out, at = [], 0
    for line, code in lines_outside_code(translation):
        if not code and re.match(r'#{1,6} ', line):
            if english[at] != anchors([line])[0]:
                out += [f'<a id="{english[at]}"></a>', '']
            at += 1
        out.append(line)
    return '\n'.join(out) + '\n'


def home(intro, japanese):
    """The README's introduction, then cards to the main sections."""
    cards = [('guides/which-api.md', 'Which API should I use?', 'どの API を使うか',
              'A live feed, a recording, the cloud API or your broker\'s BRiSK.', 'ライブフィード、記録、クラウド API、証券会社の BRiSK。'),
             ('guides/install.md', 'Install', 'インストール', 'pip, Node and the demo.', 'pip、Node、デモ。'),
             ('guides/recording.md', 'Recording', '記録する', 'What brisk live and brisk record keep and upload, and what reads it.',
              'brisk live と brisk record が何を残しアップロードし、何で読むか。'),
             ('guides/cli.md', 'Command line', 'コマンドライン', 'Every brisk command, and the Python call it stands for.',
              'brisk の全コマンドと、対応する Python の呼び出し。'),
             ('reference/historical/timeseries/get_range.md', 'Cloud API', 'クラウド API',
              'Databento-style queries over your uploads, and the public demo.', 'アップロードした記録と公開デモへの Databento 風の照会。'),
             ('reference/python/ticker_quote.md', 'Python API', 'Python API',
              'Quotes, trades and auctions from a feed or a recording.', 'フィードや記録から気配、約定、板寄せ。'),
             ('data-format.md', 'Data format', 'データ形式', 'Every column, and how to read the Parquet.', '全列と Parquet の読み方。'),
             ('privacy.md', 'Privacy', 'プライバシー', 'What is uploaded, kept and deleted.', '何をアップロードし、保存し、削除するか。')]
    grid = ['<div class="grid cards" markdown>', '']
    for page, title, title_ja, text, text_ja in cards:
        grid += [f'-   **[{title_ja if japanese else title}](/{page})**', '', '    ---', '',
                 f'    {text_ja if japanese else text}', '']
    return f'---\nhide:\n  - navigation\n  - toc\n---\n\n{intro.strip()}\n\n' + '\n'.join(grid) + '</div>\n'


def link_calls(text):
    """Link each call in the API overview's table to its reference page."""
    titles = {method.title: method.slug for _, methods in reference.REFERENCE for method in methods}

    def row(match):
        cell, rest = match[1], match[2]
        context = ''

        def call(span):   # `Ticker(code).quote(at=None)` -> Ticker.quote; `.margin(days)` continues the call before it
            nonlocal context
            name = span.strip('`')
            while re.search(r'\([^()]*\)', name):
                name = re.sub(r'\([^()]*\)', '', name)
            name = context + name if name.startswith('.') else name
            context = name.rsplit('.', 1)[0]
            slug = titles.get(name) or titles.get(name.removeprefix('briskapi.'))
            if not slug:
                return span
            return f'[{span}](/reference/{slug}.md)'

        return '| ' + re.sub(r'`[^`]+`', lambda m: call(m[0]), cell) + ' |' + rest

    return re.sub(r'^\| (`[^|]*?) \|(.*)$', row, text, flags=re.M)


def fence(language, code, indent='', numbers=False):
    head = f'```{language}' + (' linenums="1"' if numbers else '')
    return '\n'.join(indent + line if line else '' for line in [head, *code.splitlines(), '```'])


def tabs(blocks):
    """Content tabs (linked by name across the page), or the one block alone."""
    if len(blocks) == 1:
        return next(iter(blocks.values()))
    return '\n\n'.join(f'=== "{name}"\n\n{block}' for name, block in blocks.items())


def method_page(method, responses, query_url):
    returns = method.signature.rpartition('-> ')[2] if '->' in method.signature else 'None'
    left = [method.text, '']
    if method.params:
        left += ['## Parameters', '']
        for p in method.params:
            need = '<span class="param-required">required</span>' if p.required else '<span class="param-optional">optional</span>'
            left += ['<div class="param" markdown>', f'<div class="param-head"><code class="param-name">{p.name}</code> '
                     f'{need} <span class="param-sep">|</span> <span class="param-type">{html.escape(p.type)}</span></div>',
                     '', p.text, '', '</div>', '']
    left += ['## Returns', '', '<div class="param" markdown>',
             f'<div class="param-head"><span class="param-type">{html.escape(returns)}</span></div>', '', method.returns, '',
             '</div>', '']
    examples = {name: code.replace('{QUERY_URL}', query_url) for name, code in method.examples.items()}
    shown = method.responses if method.illustrative else responses.get(method.slug, {})
    missing = set(examples) - set(shown) - {'CLI', 'HTTP'}
    assert not missing, f'{method.slug}: no response for {missing}: run capture.py'
    usage = tabs({name: fence(LANGUAGES[name], code, '    ' * (len(examples) > 1), True) for name, code in examples.items()})
    result = tabs({name: fence('json' if name == 'HTTP' and text[:1] in '[{' else 'text', text, '    ' * (len(shown) > 1))
                   for name, text in shown.items()})
    note = ('<div class="card-note">Illustrative values: this call needs your broker\'s BRiSK, or your own uploads, to run.</div>'
            if method.illustrative else '')
    right = ['<div class="card" markdown>', '<div class="card-title">API method</div>', '', fence('python', method.signature), '',
             '</div>', '<div class="card" markdown>', '<div class="card-title">Example usage</div>', '', usage, '', '</div>',
             '<div class="card" markdown>', '<div class="card-title">Example response</div>', note, '', result, '', '</div>']
    return '\n'.join([f'---\ntitle: {method.title}\nhide:\n  - toc\n---\n', f'# {method.title}', '',
                      '<div class="api" markdown>', '<div class="api-text" markdown>', '', *left, '</div>',
                      '<div class="api-cards" markdown>', '', *right, '', '</div>', '</div>', ''])


def root_link(url, page):
    """A link written from the site's root (/x.md#y) made relative to the page."""
    path, _, anchor = url[1:].partition('#')
    return posixpath.relpath(path, posixpath.dirname(page) or '.') + (f'#{anchor}' if anchor else '')


def root_links(text, page):
    return re.sub(r'\]\((/[^)]+)\)', lambda m: f'](' + root_link(m[1], page) + ')', text)


def group(slug):
    return next(value for prefix, value in GROUPS.items() if slug.startswith(prefix))


def nav_yaml():
    def item(title, page, depth):
        return '  ' * depth + f'- {json.dumps(title, ensure_ascii=False)}: {page}'

    guides = {s[0].removeprefix('guides/').removesuffix('.md'): item(s[1], s[0], 1) for s in SECTIONS if s}
    lines = [item('Home', 'index.md', 0), '- "Getting started":']
    lines += [guides[name] for name in ('which-api', 'install', 'recording', 'live-feed', 'recordings', 'broker', 'passkey')]
    lines += ['- "Your archive":', guides['archive'], guides['cloud']]
    lines += ['- "Reference":', guides['cli'], guides['api-overview'], item(DOCUMENTS[0][2], DOCUMENTS[0][1], 1)]
    for title, methods in reference.REFERENCE:
        lines.append(f'- {json.dumps(title)}:')
        current = None
        for method in methods:
            name = group(method.slug)[0]
            if method.slug == 'historical/client':
                lines.append(item(method.nav, f'reference/{method.slug}.md', 1))
                continue
            if name != current:
                lines.append('  ' + f'- {json.dumps(name)}:')
                current = name
            lines.append(item(method.nav, f'reference/{method.slug}.md', 2))
    lines.append('- "About":')
    lines += [item(title, page, 1) for _, page, title, _ in DOCUMENTS[1:]]
    return '\n'.join(lines)


def translations():
    names = dict(TRANSLATIONS)
    names |= {s[1]: s[2] for s in SECTIONS if s}
    names |= {d[2]: d[3] for d in DOCUMENTS}
    names |= {value[0]: value[1] for value in GROUPS.values() if value[0] != value[1]}
    return '\n'.join(f'            {json.dumps(k, ensure_ascii=False)}: {json.dumps(v, ensure_ascii=False)}' for k, v in names.items())


def build():
    if OUT.exists():
        shutil.rmtree(OUT)
    site = Site()
    site.documents()
    site.readmes()
    responses = json.loads((HERE / 'responses.json').read_text())
    query_url = json.loads((ROOT / 'briskapi' / 'archive.json').read_text())['query_url']
    for _, methods in reference.REFERENCE:
        for method in methods:
            page = f'reference/{method.slug}.md'
            site.pages[page] = root_links(method_page(method, responses, query_url), page)
    for page, text in site.pages.items():
        (DOCS / page).parent.mkdir(parents=True, exist_ok=True)
        (DOCS / page).write_text(text)
    (DOCS / 'assets').mkdir(exist_ok=True)
    shutil.copy(HERE / 'extra.css', DOCS / 'assets' / 'extra.css')
    config = (HERE / 'mkdocs.yml').read_text().replace('{nav}', nav_yaml()).replace('{nav_translations}', translations())
    (OUT / 'mkdocs.yml').write_text(config)
    return site


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--serve', action='store_true', help='Preview at http://127.0.0.1:8000 after building')
    args = parser.parse_args()
    build()
    mkdocs = [sys.executable, '-m', 'mkdocs']
    subprocess.run([*mkdocs, 'build', '--strict', '-f', str(OUT / 'mkdocs.yml')], check=True)
    if args.serve:
        subprocess.run([*mkdocs, 'serve', '-f', str(OUT / 'mkdocs.yml')], check=True)


if __name__ == '__main__':
    main()
