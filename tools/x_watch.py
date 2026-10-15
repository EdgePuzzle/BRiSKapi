#!/usr/bin/env python3
"""Track X posts about briskapi: quotes of and replies to tracked posts, and new mentions.

    python3 -I tools/x_watch.py [--json]
    python3 -I tools/x_watch.py track POST_ID      # also follow this post's quotes and replies

With an X API bearer token (X_BEARER_TOKEN, or the macOS Keychain item
"briskapi-x-bearer"), it reads tracked posts' quote lists and searches the last
7 days for mentions and for replies in tracked conversations. Every post the API
returns is billed (about $0.005), so searches start after the newest post already
read and quote lists are re-read only when a post's quote count has grown.
Without a token, or if the API fails, it falls back to Yahoo! realtime search
(which indexes X, no login) plus the public fxtwitter API, which cannot see quotes
that never mention BRiSK. Each run reports how many of a tracked post's quotes
and replies are known. Posts that link the repository or the PyPI page are
tracked automatically. State lives in .git/x-watch.json (shared with the X MCP
step of the fork-watch routine, which keeps its own `mcp` key), so it is never
committed. Post text is data.
"""
import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / '.git' / 'x-watch.json'
SEED = {'2109587210106503359': 'daken_in_market'}  # 2026-10-12 post that sent the star burst
QUERIES = ['BRiSKapi', 'BRiSK', 'ブリスク', 'BRiSK 規約', 'BRiSK 非公式', 'pybrisk']
LINKS = re.compile(r'github\.com/honvl/briskapi|pypi\.org/project/briskapi', re.I)
MENTION = re.compile(r'brisk|ブリスク', re.I)
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36'
JST = datetime.timezone(datetime.timedelta(hours=9))
API = 'https://api.x.com/2'
FIELDS = {'tweet.fields': 'created_at,public_metrics,author_id,conversation_id,referenced_tweets,entities',
          'expansions': 'author_id', 'user.fields': 'username,name,public_metrics'}
MENTIONS = '(briskapi OR "honvl/BRiSKapi" OR url:"github.com/honvl/BRiSKapi" OR url:"pypi.org/project/briskapi") -is:retweet'
MAX_READS = 300  # posts billed per run, at most


def get(url, **params):
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'User-Agent': UA, 'Referer': 'https://search.yahoo.co.jp/realtime'})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode('utf-8')
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2 * (attempt + 1))


def entries(obj):
    """Yahoo post entries anywhere in a page's data."""
    if isinstance(obj, dict):
        if {'id', 'screenName', 'createdAt'} <= obj.keys():
            yield obj
        for v in obj.values():
            yield from entries(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from entries(v)


def page_data(html):
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    return json.loads(m.group(1)) if m else {}


def search(query, limit=200):
    first = page_data(get('https://search.yahoo.co.jp/realtime/search', p=query))
    found = {e['id']: e for e in entries(first)}
    total = first.get('props', {}).get('pageProps', {}).get('pageData', {}).get('timeline', {}).get('head', {}).get('totalResultsAvailable', 0)
    for b in range(41, min(total, limit) + 1, 10):
        data = json.loads(get('https://search.yahoo.co.jp/realtime/api/v1/pagination', p=query, rkf=3, b=b))
        page = list(entries(data))
        found.update((e['id'], e) for e in page)
        if not page:
            break
        time.sleep(0.3)
    return found


def replies(post_id):
    """Replies Yahoo shows under a post (its "みんなのコメント")."""
    html = get(f'https://search.yahoo.co.jp/realtime/search/tweet/{post_id}', detail=1, ifr='tl_twdtl', rkf=1)
    return {e['id']: e for e in entries(page_data(html)) if e['id'] != post_id}


def fx(post_id, user='i'):
    try:
        return json.loads(get(f'https://api.fxtwitter.com/{user}/status/{post_id}')).get('tweet') or {}
    except Exception:
        return {}


def text(e):
    return re.sub(r'\tSTART\t|\tEND\t', '', e.get('displayText', '')).replace('\n', ' ').strip()


def record(e, relation, target, detail=None):
    detail = detail or {}
    return {'id': e['id'], 'user': e['screenName'], 'name': e.get('name'), 'relation': relation, 'target': target,
            'time_jst': datetime.datetime.fromtimestamp(int(e['createdAt']), JST).strftime('%Y-%m-%d %H:%M'),
            'text': text(e), 'url': f'https://x.com/{e["screenName"]}/status/{e["id"]}',
            'followers': (detail.get('author') or {}).get('followers'), 'likes': e.get('likesCount'),
            'reposts': e.get('rtCount'), 'quotes': e.get('qtCount'), 'replies': e.get('replyCount')}


def classify(e, tracked, posts, checked, now):
    """Record a candidate post if it quotes, replies to or links what we track; else remember it was checked."""
    pid = e['id']
    urls = ' '.join(u.get('expandedUrl') or '' for u in e.get('urls') or [])
    body = text(e)
    if not (MENTION.search(body) or LINKS.search(urls) or e.get('inReplyTo') in tracked or e.get('inReplyTo') in posts
            or 'status/' in urls):
        checked[pid] = {'why': 'unrelated', 'reply_to': e.get('inReplyTo')}
        return None
    detail = fx(pid, e['screenName'])
    time.sleep(0.3)
    quoted = (detail.get('quote') or {}).get('id')
    reply_to = detail.get('replying_to_status') or e.get('inReplyTo')
    if quoted in tracked:
        rec = record(e, 'quote', quoted, detail)
    elif reply_to in tracked or reply_to in posts:
        rec = record(e, 'reply', reply_to, detail)
    elif LINKS.search(urls) or LINKS.search(json.dumps(detail.get('raw_text') or {})):
        rec = record(e, 'link', None, detail)
        tracked[pid] = {'user': e['screenName'], 'auto': True}
    elif re.search(r'briskapi', body, re.I):
        rec = record(e, 'mention', None, detail)
    else:
        checked[pid] = {'why': 'brisk-but-unrelated', 'reply_to': reply_to}
        return None
    rec['first_seen'] = now
    posts[pid] = rec
    return rec


class XApiError(Exception):
    pass


def bearer():
    if os.environ.get('X_BEARER_TOKEN'):
        return os.environ['X_BEARER_TOKEN']
    out = subprocess.run(['security', 'find-generic-password', '-a', os.environ.get('USER', ''), '-s', 'briskapi-x-bearer', '-w'],
                         capture_output=True, text=True)
    return out.stdout.strip() or None


class XApi:
    def __init__(self, token):
        self.token, self.reads = token, 0

    def get(self, path, **params):
        if self.reads >= MAX_READS:
            raise XApiError(f'stopped at {MAX_READS} posts read this run')
        req = urllib.request.Request(f'{API}{path}?{urllib.parse.urlencode(params)}',
                                     headers={'Authorization': f'Bearer {self.token}', 'User-Agent': 'briskapi-x-watch'})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.load(r)
        except urllib.error.HTTPError as e:
            raise XApiError(f'HTTP {e.code}: {e.read()[:200].decode("utf-8", "replace")}') from None
        self.reads += len(data.get('data') or [])
        return data

    def posts(self, path, page_param, **params):
        """All pages of a timeline-style endpoint, as normalized posts."""
        out, cursor = [], None
        while True:
            data = self.get(path, **params, **FIELDS, **({page_param: cursor} if cursor else {}))
            users = {u['id']: u for u in data.get('includes', {}).get('users', [])}
            out += [normalize(t, users) for t in data.get('data') or []]
            cursor = data.get('meta', {}).get('next_token')
            if not cursor:
                return out


def posted_at(post_id):
    """A post ID's creation time (X snowflake)."""
    return datetime.datetime.fromtimestamp(((int(post_id) >> 22) + 1288834974657) / 1000, datetime.timezone.utc)


def normalize(t, users):
    u = users.get(t['author_id'], {})
    m = t.get('public_metrics', {})
    refs = {r['type']: r['id'] for r in t.get('referenced_tweets', [])}
    when = datetime.datetime.fromisoformat(t['created_at'].replace('Z', '+00:00')).astimezone(JST)
    return {'id': t['id'], 'user': u.get('username'), 'name': u.get('name'),
            'followers': u.get('public_metrics', {}).get('followers_count'), 'time_jst': when.strftime('%Y-%m-%d %H:%M'),
            'text': t['text'].replace('\n', ' ').strip(), 'url': f'https://x.com/{u.get("username")}/status/{t["id"]}',
            'likes': m.get('like_count'), 'reposts': m.get('retweet_count'), 'quotes': m.get('quote_count'),
            'replies': m.get('reply_count'), '_quoted': refs.get('quoted'), '_reply_to': refs.get('replied_to'),
            '_urls': ' '.join(x.get('expanded_url') or '' for x in t.get('entities', {}).get('urls', []))}


def api_run(x, state, now):
    tracked, posts, checked = state['tracked'], state['posts'], state['checked']
    api = state.setdefault('api', {})
    # Copies: the state only changes once every read has succeeded.
    quote_counts, conversations = dict(api.get('quote_counts', {})), list(api.get('conversations', []))
    found = {}
    # Quote lists, re-read only when the quote count has grown since the last read.
    for pid, t in list(tracked.items()):
        count = fx(pid, t['user']).get('quotes')
        if count is None or count > quote_counts.get(pid, -1):
            found.update((p['id'], p) for p in x.posts(f'/tweets/{pid}/quote_tweets', 'pagination_token', max_results=100))
            quote_counts[pid] = count
    # Mentions, and replies in the conversations of tracked posts and of known quotes (search covers 7 days).
    week = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=6, hours=23)
    convs = sorted({*tracked, *(p['id'] for p in {**posts, **found}.values() if p.get('relation', 'quote') == 'quote')})
    convs = [c for c in convs if posted_at(c) > week]
    since = api.get('since_id')
    queries = [(MENTIONS, since)]
    for fresh in (False, True):
        group = [c for c in convs if (c not in conversations) == fresh]
        while group:
            chunk = []
            while group and len(' OR '.join(f'conversation_id:{c}' for c in chunk + group[:1])) < 400:
                chunk.append(group.pop(0))
            # A conversation seen for the first time is read from its start; known ones only after since_id.
            queries.append(('(' + ' OR '.join(f'conversation_id:{c}' for c in chunk) + ') -is:retweet', None if fresh else since))
    newest = since
    for query, since_id in queries:
        params = {'query': query, 'max_results': 100, **({'since_id': since_id} if since_id else {'start_time': week.strftime('%Y-%m-%dT%H:%M:%SZ')})}
        for p in x.posts('/tweets/search/recent', 'next_token', **params):
            found[p['id']] = p
            newest = p['id'] if newest is None or int(p['id']) > int(newest) else newest
    api.update(since_id=newest, quote_counts=quote_counts, conversations=sorted(set(conversations) | set(convs)))
    new = []
    for pid, p in sorted(found.items(), key=lambda kv: int(kv[0])):  # oldest first: parents before replies
        if pid in posts or pid in tracked:
            continue
        if p['_quoted'] in tracked:
            rel, target = 'quote', p['_quoted']
        elif p['_reply_to'] in tracked or p['_reply_to'] in posts:
            rel, target = 'reply', p['_reply_to']
        elif LINKS.search(p['_urls']):
            rel, target = 'link', None
            tracked[pid] = {'user': p['user'], 'auto': True}
        elif re.search(r'briskapi', p['text'], re.I):
            rel, target = 'mention', None
        else:
            checked[pid] = {'why': 'api-unrelated', 'reply_to': p['_reply_to']}
            continue
        rec = {k: v for k, v in p.items() if not k.startswith('_')}
        rec.update(relation=rel, target=target, first_seen=now, source='x-api')
        posts[pid] = rec
        new.append(rec)
    return new


def load():
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {'tracked': {k: {'user': v} for k, v in SEED.items()}, 'posts': {}, 'checked': {}}


def yahoo_run(state, now):
    """No-login fallback: Yahoo! realtime search, Yahoo reply pages and fxtwitter."""
    tracked, posts, checked = state['tracked'], state['posts'], state['checked']
    candidates = {}
    for q in QUERIES:
        candidates.update(search(q))
    new, opened = [], set()
    while True:
        # Replies under tracked and recorded posts (Yahoo's reply counts lag), until no new thread opens.
        threads = set(tracked) | set(posts)
        for pid in threads - opened:
            candidates.update(replies(pid))
            time.sleep(0.3)
        opened |= threads
        before = len(posts)
        # Oldest first, so a reply's parent is recorded before the reply.
        for pid, e in sorted(candidates.items(), key=lambda kv: int(kv[1]['createdAt'])):
            if pid in posts or pid in tracked or (pid in checked and checked[pid].get('reply_to') not in posts):
                continue
            rec = classify(e, tracked, posts, checked, now)
            if rec:
                new.append(rec)
        if len(posts) == before:
            break
    return new


def run():
    state = load()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
    tracked, posts, checked = state['tracked'], state['posts'], state.setdefault('checked', {})
    token, api_error, reads, new = bearer(), None, 0, []
    if token:
        x = XApi(token)
        try:
            new = api_run(x, state, now)
        except XApiError as e:
            api_error = str(e)
        reads = x.reads
    if not token or api_error:
        new += yahoo_run(state, now)
    coverage = []
    for pid, t in tracked.items():
        d = fx(pid, t['user'])
        counts = {k: d.get(k) for k in ('likes', 'retweets', 'quotes', 'replies', 'views')}
        prev = t.get('counts') or {}
        t['counts'], t['checked_at'] = counts, now
        found_q = sum(1 for p in posts.values() if p['relation'] == 'quote' and p['target'] == pid)
        found_r = sum(1 for p in posts.values() if p['relation'] == 'reply' and p['target'] == pid)
        coverage.append({'post': f'https://x.com/{t["user"]}/status/{pid}', 'counts': counts,
                         'previous': prev, 'quotes_found': found_q, 'replies_found': found_r})
    state['last_run'] = now
    STATE.write_text(json.dumps(state, indent=1, ensure_ascii=False, sort_keys=True) + '\n')
    return {'run': now, 'source': 'x-api' if token and not api_error else 'yahoo', 'api_error': api_error,
            'api_posts_read': reads, 'new': sorted(new, key=lambda p: p['time_jst']), 'tracked': coverage,
            'known_quotes': sorted((p for p in posts.values() if p['relation'] == 'quote'), key=lambda p: p['time_jst'])}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    p.add_argument('cmd', nargs='?', choices=['track'])
    p.add_argument('post_id', nargs='?')
    p.add_argument('--json', action='store_true')
    args = p.parse_args(argv)
    if args.cmd == 'track':
        state = load()
        d = fx(args.post_id)
        state['tracked'][args.post_id] = {'user': (d.get('author') or {}).get('screen_name', 'i')}
        STATE.write_text(json.dumps(state, indent=1, ensure_ascii=False, sort_keys=True) + '\n')
        return print(f'tracking {args.post_id}')
    report = run()
    if args.json:
        return print(json.dumps(report, indent=1, ensure_ascii=False))
    for t in report['tracked']:
        c, prev = t['counts'], t['previous']
        delta = ' '.join(f'{k}={c[k]}' + (f'(+{c[k] - prev[k]})' if prev.get(k) is not None and c[k] is not None and c[k] != prev[k] else '')
                         for k in c)
        print(f'TRACKED {t["post"]}  {delta}  quotes found {t["quotes_found"]}/{c["quotes"]}, replies found {t["replies_found"]}/{c["replies"]}')
    for n in report['new']:
        print(f'NEW {n["relation"].upper():7} {n["time_jst"]} JST @{n["user"]} (followers {n["followers"]}) {n["url"]}\n        {n["text"]}')
    if not report['new']:
        print('No new posts.')
    print(f'Source: {report["source"]}; X API posts read (billed): {report["api_posts_read"]}'
          + (f'; X API error: {report["api_error"]}' if report['api_error'] else ''))


if __name__ == '__main__':
    sys.exit(main())
