"""How much market data two recordings of the same session share, block by block.

Each recording's quotes and trades are cut into blocks: one security over WINDOW minutes of source time.
A block's hash covers only the market (each quote and trade as canonical JSON, the set of them, so a
snapshot that restates an update counts once), never what is a recording's own: batch numbers, local
receipt and decode times. Two users who saw the same updates of a security in a window have the same
block, whatever they started, cut or numbered. Reads a row group at a time; prints how many of each
recording's blocks, and of their bytes, the other holds too.

    python tools/block_overlap.py RECORDING_A RECORDING_B [--window 5]

RECORDING is a recording's folder (its data/*.parquet tables). Nothing leaves this computer, and the
output holds counts only.
"""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from briskapi import _parquet
from briskapi.schema import canonical


def blocks(directory, window_us, snapshot=True):
    """{(issue_id, window): (sha256, canonical bytes)} of a recording's quotes (without its opening snapshot's, the
    book where it started, unless `snapshot`), and the same of its trades."""
    directory = Path(directory)
    batches = _parquet._decode_batches(pq.read_table(directory / _parquet.BATCHES))
    midnight = _parquet.midnight_ns(batches[0]['trading_date'])
    master = _parquet._decode_equity(pq.read_table(directory / _parquet.EQUITY))
    quotes, trades = {}, {}

    class Store:
        """Blocks of rows sorted by security: each security's are finished (hashed) once its rows end, so only one
        security's rows are ever held."""
        def __init__(self, out):
            self.out, self.issue, self.open = out, None, {}

        def add(self, issue, window, text):
            if issue != self.issue:
                self.finish()
                self.issue = issue
            self.open.setdefault(window, set()).add(text)

        def finish(self):
            for window, rows in self.open.items():
                self.out[(self.issue, window)] = (hashlib.sha256('\n'.join(sorted(rows)).encode()).hexdigest(),
                                                  sum(len(r) + 1 for r in rows))
            self.open = {}

    store = Store(quotes)
    source = pq.ParquetFile(directory / _parquet.DEPTHS)
    for g in range(source.num_row_groups):
        rows = _parquet._depth_rows(source.read_row_group(g), midnight, master)
        keep = np.arange(len(rows)) if snapshot else np.flatnonzero(rows.q['seq'] != 0)
        for q in rows.quotes(keep):
            store.add(q['issue_id'], q['source_time_us'] // window_us, canonical(q))
    store.finish()
    store = Store(trades)
    source = pq.ParquetFile(directory / _parquet.TRADES)
    for g in range(source.num_row_groups):
        t, t_code = _parquet._decode_trades(source.read_row_group(g), midnight)
        rows = _parquet.Columns(None, None, None, master, batches, t, t_code)
        for trade in rows.trades_at(np.arange(len(t_code))):
            store.add(trade['issue_id'], trade['time_us'] // window_us, canonical(trade))
    store.finish()
    return quotes, trades


def compare(a, b):
    """Of a's blocks and their bytes: how many b holds the same, how many b holds differently, how many it lacks."""
    same = [k for k in a if k in b and a[k][0] == b[k][0]]
    differ = [k for k in a if k in b and a[k][0] != b[k][0]]
    size = lambda keys: sum(a[k][1] for k in keys)  # noqa: E731
    total = size(a)
    return {'blocks': len(a), 'same': len(same), 'different': len(differ), 'missing': len(a) - len(same) - len(differ),
            'bytes_same_pct': round(100 * size(same) / total, 2) if total else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('a', type=Path)
    parser.add_argument('b', type=Path)
    parser.add_argument('--window', type=float, default=5, help='Minutes of source time a block covers (default: 5)')
    parser.add_argument('--without-snapshots', action='store_true',
                        help="Leave out each recording's opening snapshot (the book where it started), kept apart in a store by blocks")
    args = parser.parse_args()
    window_us = int(args.window * 60 * 10**6)
    (qa, ta), (qb, tb) = (blocks(path, window_us, not args.without_snapshots) for path in (args.a, args.b))
    print(json.dumps({'window_minutes': args.window, 'snapshots': not args.without_snapshots,
                      'quotes': {'a_in_b': compare(qa, qb), 'b_in_a': compare(qb, qa)},
                      'trades': {'a_in_b': compare(ta, tb), 'b_in_a': compare(tb, ta)}}, indent=2))


if __name__ == '__main__':
    main()
