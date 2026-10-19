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
import tomllib
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
    ('guides/live-records.md', 'Live records', 'ライブのレコード'),
    ('guides/recordings.md', 'Recordings', '記録データ'),
    ('guides/broker.md', 'Your broker\'s BRiSK', '証券会社の BRiSK'),
    ('guides/passkey.md', 'Passkey sign-in', 'パスキーでのサインイン'),
    ('guides/api-overview.md', 'API overview', 'API 一覧'),
    ('guides/repair.md', 'Repairing a recording', '記録の補修'),
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
    'python/archive_': ('Archive', 'Archive'), 'python/broker_': ('Your broker', '証券会社'), 'python/': ('Functions', '関数'),
}
LANGUAGES = {'Python': 'python', 'HTTP': 'bash', 'CLI': 'bash'}
ALONE = ['guides/cli.md', 'data-format.md']   # pages that are a tab of their own (nav_yaml)
LANGUAGE_LINE = re.compile(r'^\[English\]\([^)]*\) \| \[日本語\]\([^)]*\).*$', re.M)
LINK = re.compile(r'(\]\()([^)\s]+)(\))')
TRANSLATIONS = {'Home': 'ホーム', 'Guides': 'ガイド', 'Getting started': 'はじめに', 'Your archive': 'アーカイブ',
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
            self.add('index.md', home(parts, japanese), name, japanese)
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


# The home page's own words, in English and Japanese; the rest of it comes from the README and the reference.
HOME = {
    'eyebrow': ('Documentation', 'ドキュメント'),
    'title': ('BRiSK auction data, in Python and the command line', 'BRiSK の板寄せデータを、Python とコマンドラインで'),
    'start': ('Get started', 'はじめる'),
    'reference': ('Cloud API reference', 'クラウド API リファレンス'),
    'query': ('Query the public demo, no account needed', '口座なしで公開デモを照会する'),
    'bars': ('One-minute bars, built in the cloud', 'クラウドで作られた 1 分足'),
    'guides': ('Getting started', 'はじめに'),
    'guides_text': ('From install to your first recording.', 'インストールから最初の記録まで。'),
    'refs': ('Reference', 'リファレンス'),
    'refs_text': ('Every call, with an example and its response.', 'すべての呼び出しを、例と応答とともに。'),
    'datasets': ('Datasets and schemas', 'データセットとスキーマ'),
    'datasets_text': ('Named like Databento\'s, venue then feed: `XTKS` is the Tokyo Stock Exchange.',
                      'Databento にならって取引所、フィードの順に名付けています。`XTKS` は東京証券取引所です。'),
    'columns': ('Dataset | Holds | Who can query it', 'データセット | 内容 | 照会できる人'),
    'brisk': ('Your own live recordings, merged across days | You', '自分のライブ記録（日をまたいでまとめたもの） | 自分だけ'),
    'demo': ('The public demo of 27 September 2021 | Every identity', '2021年9月27日の公開デモ | 誰でも'),
    'schemas': ('Schemas', 'スキーマ'),
    'only_cloud': ('OHLCV bars exist only in the cloud, built from your own uploads.',
                   'OHLCV バーはクラウドにだけあり、自分のアップロードから作られます。'),
}
# Cards to the main sections: page, icon, title, Japanese title, text, Japanese text
GUIDE_CARDS = [
    ('guides/which-api.md', 'compass-outline', 'Which API should I use?', 'どの API を使うか',
     'A live feed, a recording, the cloud API or your broker\'s BRiSK.', 'ライブフィード、記録、クラウド API、証券会社の BRiSK。'),
    ('guides/install.md', 'download', 'Install', 'インストール', 'pip, Node or Bun, and the demo.', 'pip、Node か Bun、デモ。'),
    ('guides/recording.md', 'record-circle-outline', 'Recording', '記録する',
     'What brisk live and brisk record keep and upload, and what reads it.', 'brisk live と brisk record が何を残しアップロードし、何で読むか。'),
    ('guides/cli.md', 'console', 'Command line', 'コマンドライン', 'Every brisk command, and the Python call it stands for.',
     'brisk の全コマンドと、対応する Python の呼び出し。'),
]
REFERENCE_CARDS = [
    ('reference/historical/timeseries/get_range.md', 'cloud-outline', 'Cloud API', 'クラウド API',
     'Databento-style queries over your uploads, and the public demo.', 'アップロードした記録と公開デモへの Databento 風の照会。'),
    ('reference/python/ticker_quote.md', 'language-python', 'Python API', 'Python API',
     'Quotes, trades and auctions from a feed or a recording.', 'フィードや記録から気配、約定、板寄せ。'),
    ('data-format.md', 'table', 'Data format', 'データ形式', 'Every column, and how to read the Parquet.', '全列と Parquet の読み方。'),
    ('privacy.md', 'shield-outline', 'Privacy', 'プライバシー', 'What is uploaded, kept and deleted.', '何をアップロードし、保存し、削除するか。'),
]
SOURCE_ICONS = ['play-circle-outline', 'lock-outline']   # the README's data sources, in order
SCHEMAS = [(name, name) for name in ('trades', 'mbp-10', 'definition', 'imbalance', 'brisk-quotes')] + \
          [(f'ohlcv-1{unit}', 'ohlcv-1s-ohlcv-1m-ohlcv-1h-ohlcv-1d') for unit in 'smhd']   # schema, data-format.md anchor


def home(parts, japanese):
    """The home page: the README's introduction beside a query of the demo and its captured response, the
    README's data sources, then cards to the main sections and the datasets."""
    say = {key: value[japanese] for key, value in HOME.items()}
    lines = [line for line in parts[0][1] if not line.startswith('# ') and not LANGUAGE_LINE.match(line)]
    intro = '\n'.join(line for line in lines if not line.startswith('>')).strip()
    install = '\n'.join(parts[1 + [section and section[0] for section in SECTIONS].index('guides/install.md')][1]).strip()
    requirements = install.split('\n\n')[0]
    pip = re.search(r'```sh\n([^\n#]*?)\s*(?:#[^\n]*)?\n', install)[1]

    label, *quote = [re.sub(r'^>\s?', '', line) for line in lines if line.startswith('>')]
    sources = []
    for line in quote:
        if line.startswith('- '):
            sources.append(line[2:])
        elif sources:
            sources[-1] += '\n' + line.strip()
    out = ['---', 'hide:', '  - navigation', '  - toc', '---', '', '<div class="home" markdown>',
           '<div class="home-hero" markdown>', '<div class="home-intro" markdown>',
           f'<p class="home-eyebrow">{say["eyebrow"]}</p>', f'<h1 class="home-title">{say["title"]}</h1>', '', intro, '',
           f'[{say["start"]}](/guides/install.md){{ .md-button .md-button--primary }} '
           f'[{say["reference"]}](/reference/historical/timeseries/get_range.md){{ .md-button }}', '',
           fence('sh', pip), '', requirements, '{: .home-note }', '', '</div>', *quickstart(say), '</div>', '',
           '<div class="home-section" markdown>', f'<p class="home-label">{label.strip().strip("*").rstrip(":")}</p>',
           '<div class="home-sources" markdown>']
    for i, source in enumerate(sources):
        title, body = re.match(r'\*\*(.+?):\*\*\s*(.*)', source, re.S).groups()
        title = re.sub(r'\s*\n\s*', '' if japanese else ' ', title)
        body = body if japanese else body[:1].upper() + body[1:]
        out += ['<div class="home-source" markdown>', f':material-{SOURCE_ICONS[i % len(SOURCE_ICONS)]}:{{ .source-icon }}', '',
                f'**{title}**', '', body, '</div>']
    out += ['</div>', '</div>', '']
    for key, cards in (('guides', GUIDE_CARDS), ('refs', REFERENCE_CARDS)):
        out += ['<div class="home-section" markdown>', f'<h2 class="home-heading">{say[key]}</h2>', '',
                say[f'{key}_text'], '{: .home-lede }', '', '<div class="grid cards" markdown>', '']
        for page, icon, title, title_ja, text, text_ja in cards:
            out += [f'-   :material-{icon}:{{ .card-icon }} **[{title_ja if japanese else title}](/{page})**', '',
                    f'    {text_ja if japanese else text}', '']
        out += ['</div>', '</div>', '']
    chips = ' '.join(f'[`{name}`](/data-format.md#{anchor}){{ .schema }}' for name, anchor in SCHEMAS)
    out += ['<div class="home-section" markdown>', f'<h2 class="home-heading">{say["datasets"]}</h2>', '',
            say['datasets_text'], '{: .home-lede }', '', '<div class="home-datasets" markdown>', '<div markdown>', '',
            f'| {say["columns"]} |', '| --- | --- | --- |', f'| `XTKS.BRISK` | {say["brisk"]} |',
            f'| `XTKS.BRISKDEMO` | {say["demo"]} |', '', '</div>', '<div class="home-schemas" markdown>',
            f'<p class="home-label">{say["schemas"]}</p>', '', chips, '', say['only_cloud'], '{: .home-note }', '',
            '</div>', '</div>', '</div>', '</div>', '']
    return '\n'.join(out)


def quickstart(say):
    """A card with the reference's query of the demo's one-minute bars, and the bars it returned (capture.py)."""
    bars = next(method for _, methods in reference.REFERENCE for method in methods
                if method.slug == 'historical/timeseries/get_range_bars')
    output = json.loads((HERE / 'responses.json').read_text())[bars.slug]['Python'].splitlines()
    columns = output[0].split()
    shown = ['symbol', 'open', 'high', 'low', 'close', 'volume']
    rows = []
    for line in output[2:]:   # the header, the index's name, then the bars
        day, time, *values = line.split()
        bar = dict(zip(columns, values))
        rows.append('<tr><td>' + html.escape(time.split('+')[0]) + '</td>' +
                    ''.join(f'<td>{html.escape(bar[name])}</td>' for name in shown) + '</tr>')
    table = ['<div class="response-table">', '<table class="response">', '<thead><tr><th>ts_event (JST)</th>' +
             ''.join(f'<th>{name}</th>' for name in shown) + '</tr></thead>', '<tbody>', *rows, '</tbody>', '</table>',
             '</div>']
    return ['<div class="card home-quickstart" markdown>',
            f'<div class="card-title"><span><span class="card-step">1</span>{say["query"]}</span><span>Python</span></div>', '',
            fence('python', bars.examples['Python'], numbers=True), '',
            f'<div class="card-title"><span><span class="card-step">2</span>{say["bars"]}</span></div>', *table, '</div>']


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


ENDPOINT = re.compile(r"curl (-X POST|-G) '\{QUERY_URL\}(v0/[\w.]+)'")
DEFAULT = re.compile(r'^\s+\**(\w+): [^=\n]*? = (.+?),?(?:\s+#.*)?$', re.M)   # a signature's `name: type = default,`


def api_title(title):
    """A method's title with what it belongs to muted (Historical.timeseries.get_range, Ticker.quote), and a phone
    free to wrap it after a dot."""
    owner, dot, rest = title.partition('.')
    return f'<span class="api-title-prefix">{owner}.</span><wbr>{rest.replace(".", ".<wbr>")}' if dot else title


# The reference pages' own words, per language; the method texts themselves are reference.py's and, in Japanese,
# reference.ja.json's.
LABELS = {
    'en': {'parameters': 'Parameters', 'returns': 'Returns', 'required': 'required', 'optional': 'optional',
           'method': 'API method', 'usage': 'Example usage', 'response': 'Example response', 'illustrative': 'Illustrative',
           'captured': 'Captured from the demo',
           'note': "Illustrative values: this call needs your broker's BRiSK, or your own uploads, to run."},
    'ja': {'parameters': 'パラメーター', 'returns': '戻り値', 'required': '必須', 'optional': '任意',
           'method': 'API メソッド', 'usage': '使用例', 'response': 'レスポンス例', 'illustrative': '説明用の値',
           'captured': 'デモで実行した結果',
           'note': '説明用の値です。この呼び出しの実行には、証券会社の BRiSK か、自分のアップロードが必要です。'},
}


def method_japanese(translations):
    """reference.ja.json, checked against reference.py: every method, and each of its parameters, translated."""
    for _, methods in reference.REFERENCE:
        for method in methods:
            ja = translations.get(method.slug)
            assert ja and ja.get('text') and ja.get('returns'), f'reference.ja.json lacks {method.slug}'
            assert set(ja.get('params', {})) == {p.name for p in method.params}, f'reference.ja.json: {method.slug} parameters'
    assert set(translations) == {m.slug for _, methods in reference.REFERENCE for m in methods}, 'reference.ja.json: unknown methods'
    return translations


def method_page(method, responses, query_url, ja=None):
    words = LABELS['ja' if ja else 'en']
    text, returned = (ja['text'], ja['returns']) if ja else (method.text, method.returns)
    returns = method.signature.rpartition('-> ')[2] if '->' in method.signature else 'None'
    defaults = {name: value for name, value in DEFAULT.findall(method.signature) if value != 'None'}
    left = [text, '']
    if method.params:
        left += [f"## {words['parameters']}", '']
        for p in method.params:
            need = (f'<span class="param-required">{words["required"]}</span>' if p.required else
                    f'<span class="param-optional">{words["optional"]}</span>')
            default = f' <span class="param-default">= {html.escape(defaults[p.name])}</span>' if p.name in defaults else ''
            left += ['<div class="param" markdown>', f'<div class="param-head"><code class="param-name">{p.name}</code> '
                     f'<span class="param-type">{html.escape(p.type)}</span> {need}{default}</div>',
                     '', ja['params'][p.name] if ja else p.text, '', '</div>', '']
    left += [f"## {words['returns']}", '', '<div class="param" markdown>',
             f'<div class="param-head"><span class="param-type">{html.escape(returns)}</span></div>', '', returned, '',
             '</div>', '']
    examples = {name: code.replace('{QUERY_URL}', query_url) for name, code in method.examples.items()}
    shown = method.responses if method.illustrative else responses.get(method.slug, {})
    missing = set(examples) - set(shown) - {'CLI', 'HTTP'}
    assert not missing, f'{method.slug}: no response for {missing}: run capture.py'
    usage = tabs({name: fence(LANGUAGES[name], code, '    ' * (len(examples) > 1), True) for name, code in examples.items()})
    result = tabs({name: fence('json' if name == 'HTTP' and text[:1] in '[{' else 'text', text, '    ' * (len(shown) > 1))
                   for name, text in shown.items()})
    # A response's tabs follow the example's (content.tabs.link), so the card shows none of its own; a response
    # in one language, beside examples in several, says which.
    language = f' ({next(iter(shown))})' if len(shown) == 1 and len(examples) > 1 else ''
    source = (f'<span class="card-source card-source--illustrative">{words["illustrative"]}</span>' if method.illustrative else
              f'<span class="card-source">{words["captured"]}</span>')
    note = f'<div class="card-note">{words["note"]}</div>' if method.illustrative else ''
    right = ['<div class="card" markdown>', f'<div class="card-title">{words["method"]}</div>', '', fence('python', method.signature),
             '', '</div>', '<div class="card card--usage" markdown>', f'<div class="card-title">{words["usage"]}</div>', '', usage, '',
             '</div>', '<div class="card card--response" markdown>',
             f'<div class="card-title"><span>{words["response"]}{language}</span>{source}</div>', note, '', result, '', '</div>']
    http = ENDPOINT.search(method.examples.get('HTTP', ''))
    endpoint = ''
    if http:
        verb = 'POST' if http[1] == '-X POST' else 'GET'
        endpoint = (f'<span class="endpoint"><span class="endpoint-verb endpoint-verb--{verb.lower()}">{verb}</span>'
                    f'<code>/{http[2]}</code></span>')
    meta = f'<div class="api-meta">{endpoint}<span class="api-languages">{" · ".join(examples)}</span></div>'
    return '\n'.join([f'---\ntitle: {method.title}\nhide:\n  - toc\n---\n', f'# {api_title(method.title)} {{ .api-title }}', '',
                      meta, '', '<div class="api" markdown>', '<div class="api-text" markdown>', '', *left, '</div>',
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
    """The navigation: a tab each for Home, the guides, the two APIs, the command line, the data format and the
    other documents."""
    def item(title, page, depth):
        return '  ' * depth + f'- {json.dumps(title, ensure_ascii=False)}: {page}'

    guides = {s[0].removeprefix('guides/').removesuffix('.md'): s for s in SECTIONS if s}

    def guide(name, depth):
        return item(guides[name][1], guides[name][0], depth)

    lines = [item('Home', 'index.md', 0), '- "Guides":', '  - "Getting started":']
    lines += [guide(name, 2) for name in ('which-api', 'install', 'recording', 'live-feed', 'live-records', 'recordings', 'repair', 'broker', 'passkey')]
    lines += ['  - "Your archive":', guide('archive', 2), guide('cloud', 2)]
    for title, methods in reference.REFERENCE:
        lines.append(f'- {json.dumps(title)}:')
        if title == 'Python API':   # the README's table of every call, local and cloud
            lines.append(guide('api-overview', 1))
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
    lines += [guide('cli', 0), item(DOCUMENTS[0][2], DOCUMENTS[0][1], 0), '- "About":']   # ALONE
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
    japanese = method_japanese(json.loads((HERE / 'reference.ja.json').read_text()))
    for _, methods in reference.REFERENCE:
        for method in methods:
            page = f'reference/{method.slug}.md'
            site.pages[page] = root_links(method_page(method, responses, query_url), page)
            site.pages[page.replace('.md', '.ja.md')] = root_links(
                method_page(method, responses, query_url, japanese[method.slug]), page)
    for page in ALONE:   # a tab of its own: its sidebar would list only itself
        for path in (page, page.replace('.md', '.ja.md')):
            meta, text = re.match(r'(?:---\n(.*?)\n---\n)?(.*)', site.pages[path], re.S).groups()
            assert not meta or 'hide:' not in meta, f'{path} already hides something'
            site.pages[path] = '---\n' + (meta + '\n' if meta else '') + 'hide:\n  - navigation\n---\n\n' + text.lstrip('\n')
    for page, text in site.pages.items():
        (DOCS / page).parent.mkdir(parents=True, exist_ok=True)
        (DOCS / page).write_text(text)
    (DOCS / 'assets').mkdir(exist_ok=True)
    shutil.copy(HERE / 'extra.css', DOCS / 'assets' / 'extra.css')
    shutil.copy(HERE / 'logo.svg', DOCS / 'assets' / 'logo.svg')
    shutil.copytree(HERE / 'overrides', OUT / 'overrides')
    version = tomllib.loads((ROOT / 'pyproject.toml').read_text())['project']['version']
    config = (HERE / 'mkdocs.yml').read_text().replace('{nav}', nav_yaml()).replace('{nav_translations}', translations())
    config = config.replace('{version}', json.dumps(version))
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
