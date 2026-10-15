"""Benchmark the local live-feed pipeline against pinned demo fixtures; uploads are disabled."""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from briskapi._live import Feed
from briskapi import Live


class MeasuredFeed(Feed):
    def __init__(self, *args, **kwargs):
        self.lag_ms = []
        self.quotes_applied = 0
        self.end_applied = None
        super().__init__(*args, **kwargs)

    def _apply(self, batch, new_day=False):
        super()._apply(batch, new_day)
        if batch['type'] == 'quotes':
            self.lag_ms.append(time.time_ns() / 1e6 - batch['received_unix_ms'])
            self.quotes_applied += len(batch['quotes'])
        elif batch['type'] == 'end':
            self.end_applied = time.perf_counter()


def distribution(values):
    values = sorted(values)
    return {name: round(values[min(len(values) - 1, int(len(values) * q))], 2)
            for name, q in [('p50', .5), ('p95', .95), ('p99', .99), ('max', 1)]}


def run(workers, speed, frames, cache, fast_symbols=()):
    with tempfile.TemporaryDirectory(prefix='bench-', dir=ROOT / 'cache') as folder:
        folder = Path(folder)
        environment = {**os.environ, 'BRISK_WORKERS': str(workers), 'BENCH_SPEED': str(speed), 'BENCH_FRAMES': str(frames),
                       'BRISK_MOCK_CACHE': str(cache), 'BENCH_STATS': str(folder / 'host.json'),
                       'BRISK_UPLOAD': '0', 'BRISK_IDENTITY_STORE': 'file',
                       'XDG_CONFIG_HOME': str(folder / 'config'), 'XDG_DATA_HOME': str(folder / 'data')}
        for name in ('XDG_CONFIG_HOME', 'XDG_DATA_HOME'):
            os.environ[name] = environment[name]
        startup = time.perf_counter()
        feed = MeasuredFeed(command=['node', str(ROOT / 'tools/brisk_mock/benchmark_feed.cjs')], env=environment,
                            upload=False, output=folder / 'recording', gateway=True)
        consumer = None
        fast_lag, fast_errors = [], []
        try:
            feed.ready(timeout=90)
            ready_s = time.perf_counter() - startup
            consumer_ready_s = None
            if fast_symbols:
                consumer_began = time.perf_counter()
                consumer = Live()
                consumer.subscribe('XTKS.BRISK', 'mbp-10', symbols=fast_symbols)
                def received(record):
                    if record.get('ts_recv') is not None:
                        fast_lag.append(time.time_ns() / 1e6 - record['ts_recv'].timestamp() * 1000)
                consumer.add_callback(received, fast_errors.append)
                consumer.start()
                deadline = time.monotonic() + 10
                while feed.gateway._fast_codes != set(fast_symbols):
                    assert time.monotonic() < deadline, 'Fast lane subscription did not arrive'
                    time.sleep(.01)
                while not consumer._subscriptions[0].active or any(c.lane != 'fast' for c in feed.gateway._clients):
                    assert time.monotonic() < deadline, 'Fast lane consumer did not become ready'
                    time.sleep(.01)
                consumer_ready_s = round(time.perf_counter() - consumer_began, 3)
            began = time.perf_counter()
            with feed._lanes_lock:
                feed._process.stdin.write(b'{"benchmark_start":true}\n')
                feed._process.stdin.flush()
            feed.wait(timeout=300)
            finished = time.perf_counter()
            if consumer:
                consumer.block_for_close(timeout=60)
                assert fast_lag and not fast_errors
            host = json.loads((folder / 'host.json').read_text())
            assert feed.status == 'completed' and host['summary']['quote_updates'] == feed.quotes_applied
            assert len(feed.recordings) == 1 and feed.recordings[0]['path'] and not feed.recordings[0]['error']
            elapsed = feed.end_applied - began
            result = {'workers_total': workers, 'mode': 'unpaced' if speed == 0 else f'{speed}x real time',
                      'frames': frames, 'quote_updates': feed.quotes_applied, 'source_span_s': host['source_span_ms'] / 1000,
                      'startup_s': round(ready_s, 3), 'feed_s': round(elapsed, 3),
                      'quote_updates_per_s': round(feed.quotes_applied / elapsed),
                      'real_time_headroom': round(host['source_span_ms'] / 1000 / elapsed, 2),
                      'receive_to_python_state_ms': distribution(feed.lag_ms),
                      'recording_finalization_s': round(finished - feed.end_applied, 3),
                      'peak_node_rss_mib': round(host['peak_node_rss_mib']),
                      'gateway': True, 'recording': True, 'upload': False}
            if fast_symbols:
                result.update(fast_symbols=fast_symbols, fast_records=len(fast_lag),
                              consumer_ready_s=consumer_ready_s,
                              receive_to_live_callback_ms=distribution(fast_lag))
            print(json.dumps(result), flush=True)
            return result
        finally:
            feed.close()
            if consumer:
                consumer.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache', type=Path, default=ROOT / 'cache/brisk-mock')
    parser.add_argument('--workers', nargs='+', type=int, default=[4, 8])
    parser.add_argument('--speed', type=float, default=0)
    parser.add_argument('--speeds', nargs='+', type=float)
    parser.add_argument('--frames', type=int, default=600)
    parser.add_argument('--fast-symbols', nargs='+', default=[])
    parser.add_argument('--output', type=Path, default=ROOT / 'cache/benchmark-feed.json')
    args = parser.parse_args()
    if args.frames < 4 or any(w < 0 for w in args.workers):
        parser.error('frames must be at least 4 and workers must be nonnegative')
    if args.fast_symbols and 0 in args.workers:
        parser.error('fast-symbols needs at least one quote worker')
    if any(s < 0 for s in (args.speeds or [args.speed])):
        parser.error('speeds must be nonnegative')
    assert not sys._is_gil_enabled(), 'Use free-threaded Python'
    (ROOT / 'cache').mkdir(exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for workers in args.workers:
        for speed in args.speeds or [args.speed]:
            rows.append(run(workers, speed, args.frames, args.cache.resolve(), args.fast_symbols))
            args.output.write_text(json.dumps(rows, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
