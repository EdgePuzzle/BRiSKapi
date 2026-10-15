#!/usr/bin/env python3
"""Watch the forks of honvl/BRiSKapi for work that upstream main does not have.

    python3 tools/fork_watch.py [--fetch] [--json]
    python3 tools/fork_watch.py mark OWNER/REPO:BRANCH VERDICT [NOTE]

Finds every fork (forks of forks too), compares each branch with upstream main
and lists branches with commits that are neither in main nor already reviewed.
--fetch stores those branches as refs/forks/OWNER/BRANCH so they can be read
with git; nothing from a fork is checked out or run. `mark` records a branch
head as reviewed (verdict: ported, nothing-new, declined or waiting).

Uses the gh CLI. State lives in .git/fork-watch.json, so it is never committed.
"""
import argparse
import datetime
import json
import subprocess
import sys
from pathlib import Path

UPSTREAM = 'honvl/BRiSKapi'
BASE = 'main'
VERDICTS = ('ported', 'nothing-new', 'declined', 'waiting')
ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / '.git' / 'fork-watch.json'


def gh(path):
    out = subprocess.run(['gh', 'api', '--paginate', '--slurp', path], capture_output=True, text=True)
    if out.returncode:
        raise SystemExit(f'gh api {path}: {out.stderr.strip()}')
    pages = json.loads(out.stdout)
    return [item for page in pages for item in page] if pages and isinstance(pages[0], list) else pages[0]


def load():
    return json.loads(STATE.read_text()) if STATE.exists() else {'forks': {}, 'reviewed': {}}


def save(state):
    STATE.write_text(json.dumps(state, indent=2, sort_keys=True) + '\n')


def forks():
    found, queue = [], [UPSTREAM]
    while queue:
        for fork in gh(f'repos/{queue.pop()}/forks?per_page=100'):
            found.append(fork)
            queue.append(fork['full_name'])
    return found


def scan(fetch=False):
    state = load()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
    report = {'scanned_at': now, 'new_forks': [], 'needs_review': [], 'unchanged': [], 'gone': []}
    current = set()
    for fork in forks():
        name, owner = fork['full_name'], fork['owner']['login']
        current.add(name)
        if name not in state['forks']:
            report['new_forks'].append(name)
            state['forks'][name] = {'created_at': fork['created_at'], 'first_seen': now}
        state['forks'][name]['pushed_at'] = fork['pushed_at']
        for branch in gh(f'repos/{name}/branches?per_page=100'):
            key, sha = f'{name}:{branch["name"]}', branch['commit']['sha']
            # owner:repo:branch names the fork exactly, even if an owner has several forks.
            cmp = gh(f'repos/{UPSTREAM}/compare/{BASE}...{owner}:{fork["name"]}:{branch["name"]}')
            entry = {'key': key, 'sha': sha, 'ahead_by': cmp['ahead_by'], 'behind_by': cmp['behind_by'],
                     'status': cmp['status'], 'merge_base': cmp['merge_base_commit']['sha']}
            reviewed = state['reviewed'].get(key)
            if cmp['ahead_by'] and not (reviewed and reviewed['sha'] == sha):
                entry['last_reviewed'] = reviewed
                entry['commits'] = [f'{c["sha"][:10]} {c["commit"]["author"]["date"]} {c["commit"]["author"]["name"]}'
                                    f' <{c["commit"]["author"]["email"]}> | {c["commit"]["message"].splitlines()[0]}'
                                    for c in cmp['commits']]
                entry['files'] = [f'{f["status"]} +{f["additions"]} -{f["deletions"]} {f["filename"]}'
                                  for f in cmp.get('files', [])]
                if fetch:
                    ref = f'refs/forks/{owner}/{branch["name"]}'
                    subprocess.run(['git', '-C', str(ROOT), 'fetch', '-q', '--no-tags',
                                    f'https://github.com/{name}.git', f'+refs/heads/{branch["name"]}:{ref}'], check=True)
                    entry['ref'] = ref
                report['needs_review'].append(entry)
            else:
                report['unchanged'].append(f'{key} {cmp["status"]} ahead={cmp["ahead_by"]}'
                                           + (f' reviewed={reviewed["verdict"]}' if reviewed else ''))
    report['gone'] = sorted(set(state['forks']) - current)
    state['last_scan'] = now
    save(state)
    return report


def mark(key, verdict, note):
    name, _, branch = key.partition(':')
    if not branch or verdict not in VERDICTS:
        raise SystemExit(f'usage: mark OWNER/REPO:BRANCH {{{",".join(VERDICTS)}}} [NOTE]')
    sha = next(b['commit']['sha'] for b in gh(f'repos/{name}/branches?per_page=100') if b['name'] == branch)
    state = load()
    state['reviewed'][key] = {'sha': sha, 'verdict': verdict, 'note': note,
                              'date': datetime.date.today().isoformat()}
    save(state)
    print(f'{key} {sha[:10]} marked {verdict}')


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    sub = p.add_subparsers(dest='cmd')
    m = sub.add_parser('mark')
    m.add_argument('key'); m.add_argument('verdict', choices=VERDICTS); m.add_argument('note', nargs='?', default='')
    p.add_argument('--fetch', action='store_true', help='fetch branches that need review into refs/forks/')
    p.add_argument('--json', action='store_true')
    args = p.parse_args(argv)
    if args.cmd == 'mark':
        return mark(args.key, args.verdict, args.note)
    report = scan(args.fetch)
    if args.json:
        return print(json.dumps(report, indent=2))
    print(f'Scanned {report["scanned_at"]}')
    for name in report['new_forks']:
        print(f'NEW FORK  {name}')
    for name in report['gone']:
        print(f'GONE      {name} (deleted or made private)')
    for e in report['needs_review']:
        print(f'REVIEW    {e["key"]} {e["status"]} ahead={e["ahead_by"]} behind={e["behind_by"]}'
              + (f' ref={e["ref"]}' if 'ref' in e else ''))
        for line in e['commits'] + ['files:'] + e['files']:
            print(f'            {line}')
    for line in report['unchanged']:
        print(f'ok        {line}')
    if not report['needs_review']:
        print('Nothing new to review.')


if __name__ == '__main__':
    sys.exit(main())
