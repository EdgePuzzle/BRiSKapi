"""Benchmark the local live-feed pipeline against pinned demo fixtures; uploads are disabled."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from briskapi._live import Feed
from briskapi import Live
from briskapi._recording import BriskError


lane_lag_ms = []   # how old each fast-lane line was when the feed passed it to the gateway (BENCH_LANE_LAG=1)


def _watch_lane():
    from briskapi import _gateway
    real = _gateway.Gateway.lane

    def lane(self, batch, line):
        if batch.get('lane') == 'fast' and batch.get('type') == 'quotes' and batch.get('received_unix_ms'):
            lane_lag_ms.append(time.time() * 1000 - batch['received_unix_ms'])
        return real(self, batch, line)
    _gateway.Gateway.lane = lane


if os.environ.get('BENCH_LANE_LAG'):
    _watch_lane()


class MeasuredFeed(Feed):
    _lane_split = not os.environ.get('BENCH_ONE_THREAD')   # BENCH_ONE_THREAD=1: the feed thread passes the lane's lines too, in turn
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


def _finish(consumer):
    """What a consumer in its own process measured, once the feed has ended."""
    out, _ = consumer.communicate(timeout=120)
    result = json.loads(out.strip().splitlines()[-1])
    if result.get('spent'):
        for step, (seconds, calls) in sorted(result['spent'].items(), key=lambda item: -item[1][0]):
            print(f'  consumer {step}: {seconds:.2f} s in {calls} calls ({seconds / max(calls, 1) * 1000:.3f} ms each)', file=sys.stderr)
    if os.environ.get('BENCH_LAGS'):
        Path(os.environ['BENCH_LAGS']).write_text(json.dumps(result['lag']))
    return result['lag'], [None] * result['errors']


def _poll(live, stop, took, newest, rows):
    """Ask Live.latest() every 20 ms: how long the call took, how old its newest quote was when it came back, and how many
    quotes it held."""
    import pyarrow as pa
    import pyarrow.compute as pc
    while not stop.is_set():
        began = time.perf_counter()
        try:
            table = live.latest()
        except BriskError:   # the feed has ended
            return
        took.append((time.perf_counter() - began) * 1000)
        if table.num_rows:
            age = time.time() * 1000 - pc.max(pc.cast(table.column('ts_recv'), pa.int64())).as_py() / 1e6
            if age < 3_600_000:   # a quote this feed received (before the first, ts_recv is the demo's clock of 2021)
                newest.append(age)
            rows.append(table.num_rows)
        stop.wait(0.02)


def run(workers, speed, frames, cache, fast_symbols=(), separate=False, latest=False, sized=False):
    with tempfile.TemporaryDirectory(prefix='bench-', dir=ROOT / 'cache') as folder:
        folder = Path(folder)
        environment = {**os.environ, 'BRISK_WORKERS': str(workers), 'BENCH_SPEED': str(speed), 'BENCH_FRAMES': str(frames),
                       'BRISK_MOCK_CACHE': str(cache), 'BENCH_STATS': str(folder / 'host.json'),
                       'BRISK_UPLOAD': '0', 'BRISK_IDENTITY_STORE': 'file',
                       'XDG_CONFIG_HOME': str(folder / 'config'), 'XDG_DATA_HOME': str(folder / 'data')}
        if sized and fast_symbols:   # the fast lane's workers sized for these symbols, as `brisk live --fast-symbols N` does
            environment['BRISK_FAST_SYMBOLS'] = str(len(fast_symbols))
        for name in ('XDG_CONFIG_HOME', 'XDG_DATA_HOME'):
            os.environ[name] = environment[name]
        startup = time.perf_counter()
        feed = MeasuredFeed(command=['node', str(ROOT / 'tools/brisk_mock/benchmark_feed.cjs')], env=environment,
                            upload=False, output=folder / 'recording', gateway=True)
        consumer = None
        fast_lag, fast_errors = [], []
        polling, polled, newest, held = threading.Event(), [], [], []
        try:
            feed.ready(timeout=90)
            if os.environ.get('BENCH_SLOW_CALLBACK_MS'):
                # A program in this process that is slow with its quotes (backpressure on the feed thread, by design) while a Live
                # session asks for others: the lane must not wait for it.
                slow = float(os.environ['BENCH_SLOW_CALLBACK_MS']) / 1000
                feed.on_quote(lambda quote: time.sleep(slow), codes=list(fast_symbols)[:int(os.environ.get('BENCH_SLOW_CALLBACK_SYMBOLS', 20))])
            ready_s = time.perf_counter() - startup
            consumer_ready_s = None
            if fast_symbols and separate:
                # The consumer in a process of its own, as a real one is: it reads the gateway's file from this process's
                # environment (XDG_CONFIG_HOME was set above).
                consumer_began = time.perf_counter()
                consumer = subprocess.Popen([sys.executable, str(Path(__file__).with_name('benchmark_consumer.py')), *fast_symbols],
                                            stdout=subprocess.PIPE, text=True, env={**os.environ, 'PYTHONPATH': str(ROOT)})
                assert consumer.stdout.readline().strip() == 'ready'
                deadline = time.monotonic() + 10
                while feed.gateway._fast_codes != set(fast_symbols) or any(c.lane != 'fast' for c in feed.gateway._clients):
                    assert time.monotonic() < deadline, 'Fast lane consumer did not become ready'
                    time.sleep(.01)
                consumer_ready_s = round(time.perf_counter() - consumer_began, 3)
            elif fast_symbols:
                consumer_began = time.perf_counter()
                consumer = Live()
                consumer.subscribe('XTKS.BRISK', 'mbp-10', symbols=fast_symbols, stream=not latest)
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
            if latest and consumer and not separate:
                poller = threading.Thread(target=_poll, args=(consumer, polling, polled, newest, held), daemon=True)
                poller.start()
            with feed._lanes_lock:
                feed._process.stdin.write(b'{"benchmark_start":true}\n')
                feed._process.stdin.flush()
            feed.wait(timeout=300)
            finished = time.perf_counter()
            if latest and consumer and not separate:
                polling.set()
                poller.join()
                consumer.stop()
                assert polled and newest, 'latest() answered'
            elif consumer and separate:
                fast_lag, fast_errors = _finish(consumer)
                assert fast_lag and not fast_errors
            elif consumer:
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
            if lane_lag_ms:
                result['lane_line_age_at_gateway_ms'] = distribution(lane_lag_ms)
                lane_lag_ms.clear()
            if fast_symbols and latest:
                result.update(fast_symbols=len(fast_symbols), latest_calls=len(polled), latest_quotes=max(held),
                              latest_call_ms=distribution(polled), latest_newest_quote_age_ms=distribution(newest))
            elif fast_symbols:
                result.update(fast_symbols=fast_symbols, fast_records=len(fast_lag),
                              consumer_ready_s=consumer_ready_s,
                              receive_to_live_callback_ms=distribution(fast_lag))
            print(json.dumps(result), flush=True)
            return result
        finally:
            feed.close()
            if consumer and separate:
                consumer.kill()
            elif consumer and not latest:
                consumer.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache', type=Path, default=ROOT / 'cache/brisk-mock')
    parser.add_argument('--workers', nargs='+', type=int, default=[4, 8])
    parser.add_argument('--speed', type=float, default=0)
    parser.add_argument('--speeds', nargs='+', type=float)
    parser.add_argument('--frames', type=int, default=600)
    parser.add_argument('--fast-symbols', nargs='+', default=[])
    parser.add_argument('--consumer-process', action='store_true', help='Run the Live consumer in a process of its own')
    parser.add_argument('--latest', action='store_true',
                        help='Keep the symbols fresh with subscribe(stream=False) and ask Live.latest() every 20 ms instead of streaming')
    parser.add_argument('--size-fast-lane', action='store_true', help='Give the fast lane workers for the symbols (BRISK_FAST_SYMBOLS)')
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
            rows.append(run(workers, speed, args.frames, args.cache.resolve(), args.fast_symbols, args.consumer_process,
                            args.latest, args.size_fast_lane))
            args.output.write_text(json.dumps(rows, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
