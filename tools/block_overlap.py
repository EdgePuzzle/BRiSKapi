"""How much market data two recordings of the same session share, block by block, and where they differ.

Each recording's quote updates and trades are compared row by row (a security's updates are identified by their frame number, and
are the same when the rest agrees) and counted in blocks, one security over WINDOW minutes of source time: identical in both, only
one holds some of its rows (a gap in the other), both lack some, or they disagree. It counts what `brisk repair RECORDING --from
OTHER` reports, and it is for deciding whether the archive could keep the market data many users record once: it never looks at what
is a recording's own (batch numbers, local receipt and decode times). Reads a security at a time.

    python tools/block_overlap.py RECORDING_A RECORDING_B [--window 5] [--without-snapshots]

RECORDING is a recording's folder (its data/*.parquet tables). Nothing leaves this computer, and the output holds counts only.
"""
import argparse
import json
from pathlib import Path

from briskapi import _repair


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('a', type=Path)
    parser.add_argument('b', type=Path)
    parser.add_argument('--window', type=float, default=5, help='Minutes of source time a block covers (default: 5)')
    parser.add_argument('--without-snapshots', action='store_true',
                        help="Leave out each recording's opening book and restored trades (what came before it started)")
    args = parser.parse_args()
    print(json.dumps(_repair.compare(args.a, args.b, args.window, not args.without_snapshots), indent=2))


if __name__ == '__main__':
    main()
