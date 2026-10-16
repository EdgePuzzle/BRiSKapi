"""How old briskapi.Live's records are when they reach you, in the two parts that make it up.

A Live record has two clocks: `ts_event`, when BRiSK says the update happened, and `ts_recv`, when the feed on this computer
received it. The age most people measure, this computer's clock minus `ts_event`, is both parts at once:

    BRiSK's delay    ts_recv - ts_event     the network and BRiSK's servers, and the two clocks' difference: not briskapi's
    briskapi's own   now - ts_recv          the feed's decoding, the gateway and Live's records: briskapi's

so a change in the first says nothing about the second. This prints each one's median, 95th and 99th percentile and maximum
in milliseconds, for the symbols' order books (mbp-10), while a feed runs on this computer (`brisk live --site sbi`).

    python tools/live_age.py --symbols 7203,6758 --minutes 8

Nothing leaves this computer, and the output holds counts and times only.
"""
import argparse
import time

import briskapi


def distribution(values):
    values = sorted(values)
    return {name: round(values[min(len(values) - 1, int(len(values) * q))], 1)
            for name, q in (('p50', .5), ('p95', .95), ('p99', .99), ('max', 1))}


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--symbols', default='7203', help='comma-separated codes (default 7203)')
    parser.add_argument('--minutes', type=float, default=8)
    args = parser.parse_args()
    live = briskapi.Live()
    live.subscribe(dataset='XTKS.BRISK', schema='mbp-10', symbols=args.symbols.split(','))
    since_event, since_recv, transit = [], [], []

    def measure(record):
        now_ms = time.time() * 1000
        event, received = record['ts_event'].timestamp() * 1000, record['ts_recv'].timestamp() * 1000
        since_event.append(now_ms - event)
        since_recv.append(now_ms - received)
        transit.append(received - event)
    live.add_callback(measure)
    live.start()
    live.block_for_close(timeout=args.minutes * 60)   # stops the session after that long
    if not since_event:
        raise SystemExit('No record arrived: is a feed running (brisk live --site ...), and the market open?')
    print(f'{len(since_event):,} records for {args.symbols} in {args.minutes:g} minutes (milliseconds)')
    for name, values in (('now - ts_event  (what is usually measured)', since_event),
                         ('ts_recv - ts_event  (BRiSK\'s delay and the clocks\' difference)', transit),
                         ('now - ts_recv  (briskapi\'s own)', since_recv)):
        print(f'  {name}: {distribution(values)}')


if __name__ == '__main__':
    main()
