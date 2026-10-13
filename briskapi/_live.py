"""Live consumption of the pinned public demo, plus recording and automatic upload to your private archive."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
from typing import Callable, Iterator
import warnings

from briskapi import _parquet, cli, schema
from briskapi._recording import BriskError, NotFoundError, Recording, Table, micros, quote_view
from briskapi.timing import TimingStats

DECODER = cli.DECODER
_END = object()


def _codes(codes):
    return codes if isinstance(codes, str) else ','.join(map(str, codes))


def _decoder(web, cache, codes, speed, limit_frames, node):
    if bool(web) == bool(cache):
        raise ValueError('Select exactly one of web=True or cache=DIR')
    command = [node, str(DECODER), *(['--web'] if web else ['--cache', str(cache)]), '--speed', str(speed)]
    if codes:
        command += ['--codes', _codes(codes)]
    if limit_frames:
        command += ['--limit-frames', str(limit_frames)]
    return subprocess.Popen(command, stdout=subprocess.PIPE)


def _stop(process):
    if process.poll() is None:
        process.kill()
    process.stdout.close()
    process.wait()


def stream(web=False, cache=None, codes=None, speed=1, limit_frames=None, node='node') -> Iterator[dict]:
    """Yield raw decoded batches as each frame replays (bootstrap, quote deltas, end)."""
    cli.check_node(node)
    process = _decoder(web, cache, codes, speed, limit_frames, node)
    try:
        for line in process.stdout:
            yield json.loads(line)
    finally:
        _stop(process)
    if process.returncode:
        raise BriskError(f'Decoder exited with {process.returncode}')


def _notice(value, created):
    from briskapi import identity
    warnings.warn('Complete sessions upload to your private BRiSK archive by default: a free off-computer copy only '
                  f'your identity can reach (owner {cli.owner(value)}, kept in {" and ".join(identity.where())}). '
                  + ('Keep its recovery code: `brisk identity --recovery-code`. ' if created else '')
                  + 'Opt out with upload=False, briskapi.consent(revoke=True) or BRISK_UPLOAD=0; see '
                  'https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md', stacklevel=6)


def _consent(upload, limit_frames):
    """(whether to package for upload, whether to upload) for a session: complete sessions upload by default
    (said once per process) or as you chose with briskapi.consent, unless this run opts out."""
    choice = cli.consented(upload, ask=False, notify=_notice)
    allowed = bool(choice and choice['enabled']) and limit_frames is None
    if upload and not allowed:
        raise BriskError('upload=True needs uploads on (briskapi.consent(accept=True)) and a complete replay')
    return allowed, allowed and not cli.opted_out(upload)


class Feed:
    """Live auction state, updated in a background thread as the decoder emits frames.

        with briskapi.connect(web=True, codes=["7203", "6758"]) as feed:
            feed.on_quote(lambda q: print(q["code"], q["indicative_price"]))
            for q in feed.quotes("7203"):        # blocks for each update
                ...
            briskapi.Ticker("7203").quote()         # current state, any time

    Consumers apply backpressure: a slow `quotes()` iterator or callback slows the
    feed instead of dropping updates. A complete session is uploaded to your private
    archive automatically, by default or as you chose with briskapi.consent (see
    PRIVACY.md); pass upload=False to keep it local.
    """

    manifest = None

    def __init__(self, web=False, cache=None, codes=None, speed=1, limit_frames=None, upload=None,
                 history=False, node='node', *, command=None, env=None, timing=None, upload_market_data=None, output=None,
                 on_day=None):
        # A broker session (SBI, e-shiten) uploads its timing summary and its market recording;
        # upload_market_data=False keeps the market recording local.
        if upload_market_data is not None and (command is None or timing not in schema.BROKER_TRANSPORT):
            raise BriskError('upload_market_data applies to a broker live session (SBI or e-shiten)')
        if upload is False and upload_market_data:
            raise BriskError('upload=False keeps the whole session local; drop upload_market_data=True')
        cli.check_node(command[0] if command else node)
        if command is None:
            self._package, self._timing = _consent(upload, limit_frames)[1], None
        else:
            wanted = upload if upload is not None else (True if upload_market_data else None)
            uploads = _consent(wanted, None)[1] if timing else False
            self._package = uploads and upload_market_data is not False
            self._timing = timing if uploads else None  # the source whose timing summaries are uploaded
        # Each finished day (bootstrap ... end) is packaged, kept in `output` as SOURCE_YYYYMMDD_FROM-TO
        # and uploaded, on a worker thread while the feed goes on.
        self._output = Path(output) if output else None
        if self._output and limit_frames is not None:
            raise BriskError('A partial replay (limit_frames) is not a recording to keep; drop output')
        self._stats, self._on_day = None, on_day
        self.recordings: list[dict] = []  # one per finished day: trading_date, name, path, uploaded, timing_uploaded
        self.timing_uploaded = None
        self._history = {} if history else None
        self._trade_history = []
        self._quotes: dict[int, dict] = {}
        self._bootstrap = None
        self._last_source_time_us = None
        self._listeners: list = []
        self._changed = threading.Condition()
        self._closing = False
        self.status, self.error, self.seq, self.uploaded = 'starting', None, -1, None
        self._tmp = tempfile.TemporaryDirectory() if self._package or self._output else None
        self._worker = ThreadPoolExecutor(1, thread_name_prefix='brisk-day')
        self._process = (subprocess.Popen(command, stdout=subprocess.PIPE, env=env) if command
                         else _decoder(web, cache, codes, speed, limit_frames, node))
        self._thread = threading.Thread(target=self._run, name='brisk-feed', daemon=True)
        self._thread.start()

    def __repr__(self):
        return f'Feed(status={self.status!r}, seq={self.seq})'

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # Recording-compatible reads used by Ticker and Market.
    @property
    def bootstrap(self) -> dict:
        self.ready()
        return self._bootstrap

    @property
    def trading_date(self) -> str:
        return self.bootstrap['trading_date']

    @property
    def source(self) -> str:
        return self.bootstrap['source']

    @property
    def clock_range(self) -> tuple[int, int]:
        """First and most recently received batch clocks."""
        self.ready()
        with self._changed:
            return self._bootstrap['source_time_us'], self._last_source_time_us

    @property
    def codes(self) -> list[str]:
        return [m['code'] for m in self.bootstrap['master']]

    def issue(self, code) -> dict:
        for entry in self.bootstrap['master']:
            if entry['code'] == str(code):
                return entry
        raise NotFoundError(f'{code} is not in this feed')

    def state(self, at=None) -> dict[int, dict]:
        """Current raw quote per issue. Live feeds hold only the present; record() for time travel."""
        if at is not None:
            raise ValueError('A live feed holds current state only; use a Recording for past times')
        self.ready()
        with self._changed:
            return dict(self._quotes)

    def trades(self, code=None, start=None, end=None) -> list[dict]:
        """Raw trades received so far, of one security or all, in source-time order (needs history=True)."""
        if self._history is None:
            raise BriskError('Pass history=True to briskapi.connect() to retain trades')
        issue = None if code is None else self.issue(code)['issue_id']
        low, high = micros(self.trading_date, start), micros(self.trading_date, end)
        with self._changed:
            retained = list(self._trade_history)
        return sorted((t for t in retained if (issue is None or t['issue_id'] == issue)
                       and (low is None or t['time_us'] >= low) and (high is None or t['time_us'] <= high)),
                      key=lambda t: t['time_us'])

    def frames(self):
        raise BriskError('A live feed keeps no frames; record the session or load a recording')

    def updates(self, code, start=None, end=None) -> Iterator[dict]:
        """Raw updates received so far for one security (needs history=True)."""
        if self._history is None:
            raise BriskError('Pass history=True to briskapi.connect() to retain updates')
        issue = self.issue(code)['issue_id']
        low, high = micros(self.trading_date, start), micros(self.trading_date, end)
        with self._changed:
            retained = list(self._history.get(issue, ()))
        return (q for q in retained if (low is None or q['source_time_us'] >= low)
                and (high is None or q['source_time_us'] <= high))

    # Live consumption.
    def ready(self, timeout=60) -> Feed:
        """Wait until the bootstrap (master and initial quotes) has been applied."""
        with self._changed:
            if not self._changed.wait_for(lambda: self._bootstrap is not None or self.status in {'failed', 'closed', 'completed'}, timeout):
                raise TimeoutError('Feed did not start in time')
        if self._bootstrap is None:
            raise BriskError(f'Feed {self.status} before bootstrap: {self.error}')
        return self

    def on_quote(self, callback: Callable[[dict], object], codes=None, raw=False) -> Callable[[], None]:
        """Call `callback(quote)` on the feed thread for each update; returns an unsubscribe function.

        The callback first receives each selected security's current quote.
        An exception in a callback stops the feed and is re-raised by wait().
        """
        return self._listen(callback, codes, raw)[1]

    def quotes(self, codes=None, raw=False, timeout=None) -> Iterator[dict]:
        """Iterate updates as they arrive, starting with each selected security's current quote."""
        inbox: queue.Queue = queue.Queue(maxsize=10_000)
        active = [True]

        def deliver(item):
            # Backpressure, but never wedge the feed on an abandoned iterator.
            while active[0]:
                try:
                    return inbox.put(item, timeout=0.2)
                except queue.Full:
                    pass

        current, remove = self._listen(deliver, codes, raw, end=lambda: deliver(_END), replay=False)
        try:
            yield from current
            while (item := inbox.get(timeout=timeout)) is not _END:
                yield item
        except queue.Empty:
            raise TimeoutError('No update within timeout') from None
        finally:
            active[0] = False
            remove()
        self._raise()

    def snapshot(self, raw=False) -> Table:
        state, date = self.state(), self.trading_date
        return Table(state[i] if raw else quote_view(state[i], date) for i in sorted(state))

    def wait(self, timeout=None) -> Feed:
        """Block until the replay ends (and any upload finishes); raise if it failed."""
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError('Feed still running')
        self._raise()
        return self

    def close(self):
        """Stop the decoder. A session closed before its end is never uploaded."""
        self._closing = True
        if self._process.poll() is None:
            self._process.kill()
        self._thread.join()

    def _raise(self):
        if self.status == 'failed':
            raise BriskError(f'Feed failed: {self.error}') from self.error

    def _listen(self, deliver, codes, raw, end=None, replay=True):
        self.ready()
        wanted = None if codes is None else {self.issue(c)['issue_id'] for c in ([codes] if isinstance(codes, str) else codes)}
        date = self._bootstrap['trading_date']
        listener = (wanted, deliver, (lambda q: q) if raw else (lambda q: quote_view(q, date)), end)
        with self._changed:
            # Snapshot and registration are atomic, so no update is missed or reordered.
            current = [listener[2](self._quotes[i]) for i in sorted(self._quotes) if wanted is None or i in wanted]
            if self.status == 'running':
                self._listeners.append(listener)
            elif end:
                end()
            if replay:
                for quote in current:
                    deliver(quote)

        def remove():
            with self._changed:
                if listener in self._listeners:
                    self._listeners.remove(listener)
        return current, remove

    def _run(self):
        day, days, ended = None, 0, False
        try:
            for line in self._process.stdout:
                batch = json.loads(line)
                # Each day runs bootstrap ... end, numbered from its own bootstrap; a new day follows an end.
                first = batch.get('type') == 'bootstrap'
                if batch.get('seq') != (0 if first else self.seq + 1) or first != (self.seq < 0 or ended):
                    raise BriskError(f'Stream sequence error at {batch.get("seq")}')
                if first:
                    days += 1
                    if self._tmp:
                        day = Path(self._tmp.name) / f'day-{days}'
                        day.mkdir()
                        day = (day, open(day / 'events.jsonl', 'wb'))
                    self._stats = TimingStats(self._timing) if self._timing else None
                if day:
                    day[1].write(line)
                ended = batch['type'] == 'end'
                if self._stats:
                    self._stats.add(batch)
                self._apply(batch, new_day=first and days > 1)
                if ended:
                    if day:
                        day[1].close()
                    self._worker.submit(self._finish_day, day and day[0], self._stats)
                    day, self._stats = None, None
            if self._closing:
                raise BriskError('closed')
            if self._process.wait():
                raise BriskError(f'Decoder exited with {self._process.returncode}')
            if not ended:
                raise BriskError('Decoder closed before replay completion')
            with self._changed:
                self.status = 'completed'
                self._changed.notify_all()
            self._finish()
        except Exception as error:  # noqa: BLE001 - every failure ends the feed visibly
            with self._changed:
                self.status, self.error = ('closed', None) if self._closing else ('failed', error)
                self._changed.notify_all()
            self._finish()
        finally:
            _stop(self._process)
            if day:
                day[1].close()  # an unfinished day's market data is never kept or uploaded
            if self._stats and self.status in {'completed', 'closed'}:
                self.timing_uploaded = self._upload_timing(self._stats)
            self._worker.shutdown(wait=True)  # finished days are still kept and uploaded
            if self._tmp:
                self._tmp.cleanup()

    def _apply(self, batch, new_day=False):
        quotes = batch.get('quotes', ())
        with self._changed:
            if new_day:  # the next trading day's state replaces the last one's
                self._quotes.clear()
                self._trade_history.clear()
                if self._history is not None:
                    self._history.clear()
            if batch['type'] == 'bootstrap':
                self._bootstrap = {k: v for k, v in batch.items() if k != 'quotes'}
                self.status = 'running'
            self._quotes.update((q['issue_id'], q) for q in quotes)
            if self._history is not None:
                for q in quotes:
                    self._history.setdefault(q['issue_id'], []).append(q)
                self._trade_history.extend(batch.get('trades', ()))
            self.seq = batch['seq']
            self._last_source_time_us = batch['source_time_us']
            listeners = list(self._listeners)
            self._changed.notify_all()
        for wanted, deliver, view, _ in listeners:
            for q in quotes:
                if wanted is None or q['issue_id'] in wanted:
                    deliver(view(q))

    def _finish(self):
        with self._changed:
            listeners, self._listeners = self._listeners, []
        for *_, end in listeners:
            if end:
                end()

    def _finish_day(self, directory, stats):
        """Package a finished day, keep it in `output` and upload it and its timing summary, as chosen."""
        result = {'trading_date': self.trading_date, 'name': None, 'path': None, 'uploaded': None, 'timing_uploaded': None}
        try:
            if directory:
                package = directory / 'package'
                try:
                    m = cli.package(directory / 'events.jsonl', package)
                    result['name'] = m and schema.local_name(m['summary'])
                    if self._output:
                        target = self._output / result['name']
                        if target.exists():
                            warnings.warn(f'{target} already exists; this day was not kept')
                        else:
                            self._output.mkdir(parents=True, exist_ok=True)
                            shutil.copytree(package, target)
                            result['path'] = str(target)
                    if self._package:
                        result['uploaded'] = cli.upload(package, cli.settings()['api_url'], verify=False, out=sys.stderr)
                except Exception as error:  # noqa: BLE001 - market data was delivered; report, do not fail
                    result['uploaded'] = {'status': 'failed', 'error': str(error)}
                    warnings.warn(f'Automatic upload failed: {error}')
            if stats:
                result['timing_uploaded'] = self._upload_timing(stats)
        finally:
            if directory:
                shutil.rmtree(directory, ignore_errors=True)
        self.uploaded, self.timing_uploaded = result['uploaded'], result['timing_uploaded']
        self.recordings.append(result)
        if self._on_day:
            try:
                self._on_day(result)
            except Exception as error:  # noqa: BLE001 - a reporting callback must not stop the recording
                warnings.warn(f'on_day callback failed: {error}')

    def _upload_timing(self, stats):
        try:
            report = stats.report()
            return cli.upload_timing(report, cli.settings()['api_url']) if report else None
        except Exception as error:  # noqa: BLE001 - the session itself succeeded; report, do not fail
            warnings.warn(f'Timing upload failed: {error}')
            return {'status': 'failed', 'error': str(error)}


def connect(web=False, cache=None, codes=None, speed=1, limit_frames=None, upload=None, history=False,
            node='node', timeout=60, output=None, on_day=None) -> Feed:
    """Start a live feed and return once its bootstrap state is available. `output` keeps the finished
    recording there (as SOURCE_YYYYMMDD_FROM-TO); `on_day(result)` is called once it is kept and uploaded."""
    feed = Feed(web, cache, codes, speed, limit_frames, upload, history, node, output=output, on_day=on_day)
    try:
        return feed.ready(timeout)
    except BaseException:
        feed.close()
        raise


def record(output, web=False, cache=None, codes=None, speed=1, limit_frames=None, upload=None,
           binary=None) -> Recording:
    """Record a demo replay into the directory `output` (Parquet tables, see briskapi._parquet) and
    upload it to your private archive according to saved consent.

    upload=None uploads by default or as you chose with `briskapi.consent()` (BRISK_UPLOAD=0 opts
    out), False keeps the recording local, True requires uploads on. Partial replays (limit_frames)
    always stay local.
    """
    output = Path(output)
    packaged, upload = _consent(upload, limit_frames)
    with tempfile.TemporaryDirectory() as tmp:
        events = Path(tmp) / 'events.jsonl'
        cli.record_events(events, web, cache, codes and _codes(codes), limit_frames, speed, binary)
        if not packaged:
            if (output / _parquet.DEPTHS).exists():
                raise BriskError(f'A recording already exists in {output}')
            cli.save_recording(events, output)
            return Recording(output)
        cli.package(events, output)
    recording = Recording(output)
    if upload:
        recording.uploaded = cli.upload(output, cli.settings()['api_url'], verify=False)
    return recording
