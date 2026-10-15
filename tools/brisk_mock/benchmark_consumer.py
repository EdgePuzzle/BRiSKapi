"""A Live consumer in a process of its own, for benchmark_feed.py --consumer-process: subscribes to mbp-10 (BENCH_SCHEMA: another) of the symbols
given (XTKS.BRISK) and prints, when the feed ends, how late each record's callback ran (ms after the record's local
receipt) and, with BENCH_PROFILE=1, how long the steps of a batch took on the session's thread."""
import json
import os
import sys
import time

from briskapi import Live, _gateway, _parquet, records

lag, errors, spent = [], [], {}


def timed(owner, name, label=None):
    original = getattr(owner, name)

    def wrapper(*args, **kwargs):
        began = time.perf_counter()
        try:
            return original(*args, **kwargs)
        finally:
            total = spent.setdefault(label or name, [0.0, 0])
            total[0] += time.perf_counter() - began
            total[1] += 1
    setattr(owner, name, wrapper)


if os.environ.get('BENCH_PROFILE'):
    for owner, name in ((Live, '_take'), (Live, '_subscribed'), (Live, '_new_day'), (_gateway._Day, '__init__'), (_parquet, 'equity_table'), (Live, '_project'), (Live, '_deliver'), (_parquet, 'stream_tables'),
                        (records, 'project'), (_gateway.Live, '_rows'), (_gateway, '_only'), (_gateway, '_ordered'), (_gateway, '_bar_table')):
        timed(owner, name)
    real_loads = json.loads
    _gateway.json = type('J', (), {'loads': staticmethod(lambda line: (lambda began: (real_loads(line), spent.setdefault(
        'json.loads', [0.0, 0]).__setitem__(0, spent['json.loads'][0] + time.perf_counter() - began),
        spent['json.loads'].__setitem__(1, spent['json.loads'][1] + 1))[0])(time.perf_counter())),
        'dumps': staticmethod(json.dumps)})

live = Live()
live.subscribe('XTKS.BRISK', os.environ.get('BENCH_SCHEMA', 'mbp-10'), symbols=sys.argv[1:])
live.add_callback(lambda record: lag.append(time.time_ns() / 1e6 - record['ts_recv'].timestamp() * 1000)
                  if record.get('ts_recv') is not None else None, errors.append)
live.start()
print('ready', flush=True)
live.block_for_close(timeout=300)
print(json.dumps({'lag': lag, 'errors': len(errors), 'spent': spent}), flush=True)
