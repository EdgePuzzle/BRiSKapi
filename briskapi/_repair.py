"""Two recordings of one trading day, compared row by row, and one repaired from the other.

Two feeds of the same day (two brokers, two computers, a restart) record the same market through the same decoder, so a
quote update carries the server's frame number whichever of them saw it: an update is identified by its security and
frame, and is the same update when everything else it holds agrees. Compared that way each row of one recording is
SAME as the other's, in CONFLICT with it (the frame is there with other content), ONLY this one's (the other lacks the
frame though it was running), OUTSIDE (before the other started or after it ended) or, for the book a recording opens
with and the trades a late start restores, a SNAPSHOT. A feed that dropped and reconnected lacks the frames of the gap
(they are ONLY the other's); two feeds that see different data conflict. The same counted over blocks, a security's rows
in a window of source time, says how much of a day two recordings could share.

`repair` writes a copy of a recording with the updates and trades only another feed's recording holds added, each frame as a
batch of its own (with the other's receipt time). What the recording holds stays as it is, so a conflict is decided for it.
Recordings that cover mostly the same time are feeds of one stretch and repair each other; recordings with different times
(a morning's, an afternoon's) are pieces one after the other, and a repair goes as far as the next piece starts and no
further, to close the gap a reconnect left but never to store the day twice. The result is verified like any broker recording.

Both recordings are read a security at a time (their tables are sorted by security) and what is written goes out a row
group at a time, so no day is ever whole in memory. A row's identity is a 64-bit hash of its content: it only has to tell
two recordings' rows apart.
"""
from __future__ import annotations

from pathlib import Path
import shutil
import tempfile

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from briskapi import _parquet
from briskapi._parquet import BOOKS, COUNTS, DEPTHS, PRICES, ROW_GROUP, TRADES
from briskapi._recording import micros
from briskapi.schema import require, site_of

SAME, CONFLICT, ONLY, OUTSIDE, SNAPSHOT = range(5)
CLASSES = ('same', 'conflict', 'only', 'outside', 'snapshot')
BLOCKS = ('identical', 'a_lacks', 'b_lacks', 'each_lacks', 'conflict', 'unshared')
RESTORED = ('bootstrap', 'history')   # the batches that restate what came before a recording: a book, the day's trades
_PRIME, _SHIFT, _ZERO = np.uint64(0x9E3779B97F4A7C15), np.uint64(32), np.zeros(1, np.uint64)


def _mix(h, x):
    """h folded with the integers x (an array): a multiplicative hash, wrapping at 64 bits."""
    h = (h ^ x.astype(np.uint64, copy=False)) * _PRIME
    return h ^ (h >> _SHIFT)


def clock(us):
    """A source time as the exchange's clock reads it: HH:MM:SS.mmm."""
    s, micro = divmod(int(us), 10**6)
    return f'{s // 3600:02d}:{s // 60 % 60:02d}:{s % 60:02d}.{micro // 1000:03d}'


class Side:
    """A recording being compared: its batches, and where in the day it lies."""

    def __init__(self, directory):
        self.directory = Path(directory)
        require(_parquet._format(self.directory) == _parquet.SCHEMA,
                f'{self.directory} is not in the current recording format (briskapi 1.5 or later writes it)')
        self.batches = _parquet._decode_batches(pq.read_table(self.directory / _parquet.BATCHES))
        require(self.batches[-1]['type'] == 'end', f'{self.directory} has no end: package or repair it first')
        self.date = self.batches[0]['trading_date']
        self.midnight = _parquet.midnight_ns(self.date)
        self.clock = np.array([b['source_time_us'] for b in self.batches], np.int64)
        self.kind = np.array([b['type'] for b in self.batches])
        self.lo, self.hi = int(self.clock[0]), int(self.clock[-1])
        self.site = site_of(self.batches[0]['input_transport'])
        self.restoring = next((i for i, b in enumerate(self.batches) if b['type'] not in RESTORED), len(self.batches))

    def restored(self, seq):
        """Which rows, given their batches' numbers, restate what came before the recording."""
        return np.isin(self.kind[seq], RESTORED)

    def describe(self):
        return {'trading_date': self.date, 'site': self.site, 'from': clock(self.lo), 'to': clock(self.hi),
                'frames': int((self.kind == 'quotes').sum())}


def by_issue(path):
    """(issue_id, table of its rows) for each security of a recording's depths or trades table, in order."""
    source, pending, issue = pq.ParquetFile(path), [], None
    for g in range(source.num_row_groups):
        table = source.read_row_group(g)
        ids = table.column('issue_id').to_numpy()
        if not len(ids):
            continue
        starts = np.concatenate([[0], np.flatnonzero(ids[1:] != ids[:-1]) + 1])
        for start, end in zip(starts.tolist(), np.concatenate([starts[1:], [len(ids)]]).tolist()):
            if pending and int(ids[start]) != issue:
                yield issue, pa.concat_tables(pending)
                pending = []
            issue = int(ids[start])
            pending.append(table.slice(start, end - start))
    if pending:
        yield issue, pa.concat_tables(pending)


def paired(a, b):
    """(issue, a's table or None, b's table or None) for each security in either, in order."""
    left, right = next(a, None), next(b, None)
    while left is not None or right is not None:
        if right is None or (left is not None and left[0] < right[0]):
            yield left[0], left[1], None
            left = next(a, None)
        elif left is None or right[0] < left[0]:
            yield right[0], None, right[1]
            right = next(b, None)
        else:
            yield left[0], left[1], right[1]
            left, right = next(a, None), next(b, None)


class Rows:
    """A security's rows in one recording: their frame, source time and batch, and a hash of the rest. `table` is None, and the
    arrays empty, for a recording that has none of the security's."""

    def __init__(self, table=None, frame=None, time=None, seq=None, sig=None):
        zero = np.zeros(0, np.int64)
        self.table = table
        self.frame, self.time, self.seq = (zero if v is None else v for v in (frame, time, seq))
        self.sig = np.zeros(0, np.uint64) if sig is None else sig

    def __len__(self):
        return len(self.frame)

    def only(self, keep):
        """These of the rows (those `keep`, a mask) as a Rows without a table."""
        return Rows(None, self.frame[keep], self.time[keep], self.seq[keep], self.sig[keep])


def quotes(table, side):
    """A security's quote updates from rows of the depths table; their hash covers everything the update holds but its batch."""
    if table is None:
        return Rows()
    d = _parquet._decode_depths(table, side.midnight)
    h = np.zeros(len(d['frame']), np.uint64)
    for key in ('frame', 'source_time_us', 'special_quote_time_us', 'turnover10', *PRICES, *COUNTS):
        h = _mix(h, d[key])
    for name in BOOKS:
        counts, levels = d[f'{name}.counts'], d[f'{name}.levels']
        bounds = np.concatenate([[0], np.cumsum(counts)])
        place = np.arange(len(levels)) - np.repeat(bounds[:-1], counts)
        level = _mix(_mix(_mix(_mix(np.zeros(len(levels), np.uint64), place), levels[:, 0]), levels[:, 1]), levels[:, 2])
        sums = np.concatenate([_ZERO, np.cumsum(level, dtype=np.uint64)])
        h = _mix(_mix(h, sums[bounds[1:]] - sums[bounds[:-1]]), counts)
    return Rows(table, d['frame'], d['source_time_us'], d['seq'], h)


def trades(table, side):
    """A security's trades from rows of the trades table, hashed by frame: a frame's trades count together (all of them are
    there or none, and a missed trade would not leave its neighbours)."""
    if table is None:
        return Rows()
    t, _ = _parquet._decode_trades(table, side.midnight)
    order = np.argsort(t['frame'], kind='stable')
    frames = t['frame'][order]
    first = np.concatenate([[True], frames[1:] != frames[:-1]])
    start = np.flatnonzero(first)
    size = np.diff(np.concatenate([start, [len(frames)]]))
    place = np.arange(len(frames)) - np.repeat(start, size)
    h = np.zeros(len(frames), np.uint64)
    for key in ('time_us', 'price10', 'quantity', 'side'):
        h = _mix(h, t[key][order])
    sums = np.concatenate([_ZERO, np.cumsum(_mix(h, place), dtype=np.uint64)])
    group = _mix(sums[start + size] - sums[start], size)
    sig = np.empty(len(frames), np.uint64)
    sig[order] = np.repeat(group, size)
    return Rows(table, t['frame'], t['time_us'], t['seq'], sig)


def classify(x, y, lo, hi, snap):
    """Each of x's rows against y's: SAME, CONFLICT, ONLY, OUTSIDE (x's time is outside y's range `lo`..`hi`) or SNAPSHOT (`snap`,
    a mask of x's rows restating what came before, or None to count them as any)."""
    out = np.full(len(x), ONLY, np.int8)
    out[(x.time < lo) | (x.time > hi)] = OUTSIDE
    if len(y):
        out[np.isin(x.frame, y.frame)] = CONFLICT
        out[np.isin(x.sig, y.sig)] = SAME
    if snap is not None:
        out[snap] = SNAPSHOT
    return out


def block_status(a, b):
    """The status of a block (BLOCKS' names) from the counts of each class among its rows in a and in b."""
    if a[CONFLICT] or b[CONFLICT]:
        return 'conflict'
    if a[ONLY] and b[ONLY]:
        return 'each_lacks'
    if b[ONLY]:
        return 'a_lacks'
    if a[ONLY]:
        return 'b_lacks'
    return 'identical' if a[SAME] or b[SAME] else 'unshared'


class Tally:
    """What a comparison counts: the rows of each class in each recording, and blocks (a security over a window) by status."""

    def __init__(self):
        self.rows = {side: dict.fromkeys(CLASSES, 0) for side in 'ab'}
        self.blocks = dict.fromkeys(BLOCKS, 0)
        self.block_rows = dict.fromkeys(BLOCKS, 0)

    def add(self, a, a_class, b, b_class, window):
        for side, cls in (('a', a_class), ('b', b_class)):
            for name, n in zip(CLASSES, np.bincount(cls, minlength=len(CLASSES)).tolist()):
                self.rows[side][name] += n
        a_window, b_window = a.time // window, b.time // window
        windows = np.union1d(a_window, b_window)
        counts = [np.bincount(np.searchsorted(windows, w) * len(CLASSES) + cls, minlength=len(windows) * len(CLASSES))
                  .reshape(-1, len(CLASSES)) for w, cls in ((a_window, a_class), (b_window, b_class))]
        for i in range(len(windows)):
            status = block_status(counts[0][i], counts[1][i])
            self.blocks[status] += 1
            self.block_rows[status] += int(counts[0][i].sum() + counts[1][i].sum())

    def report(self):
        total = sum(self.block_rows.values())
        return {'rows': self.rows, 'blocks': self.blocks,
                'rows_in_identical_blocks_pct': round(100 * self.block_rows['identical'] / total, 2) if total else None}


class Gaps:
    """What repair adds, security by security: the other's rows (Arrow tables, with their batches' numbers); and what it leaves."""

    def __init__(self):
        self.quotes, self.trades, self.recorded = {}, {}, set()
        self.left = {'misplaced': 0, 'after_end': 0, 'restored': 0, 'not_recorded': 0}

    def batches(self):
        """The other's batches that hold any of them, in order."""
        return sorted({int(s) for tables in (self.quotes, self.trades) for _, seq in tables.values() for s in seq})


def fit(a, b, candidates, a_clock, b_clock, lo, hi):
    """Which of b's rows (those in the mask `candidates`) can be added among a's: inside a's range `lo`..`hi`, after a's first
    row of the security, and between the rows around it in frame, time and batch clock, so that adding it breaks no order the
    archive checks (a security with none of a's rows has none to keep in order with). Returns their mask, how many in range do not fit and how many are out of range."""
    at = np.flatnonzero(candidates)
    frame, time, clk = b.frame[at], b.time[at], b_clock[at]
    inside = (clk >= lo) & (clk <= hi)
    if not len(a):   # nothing of the security's to keep in order with
        out = np.zeros(len(candidates), bool)
        out[at[inside]] = True
        return out, 0, int((~inside).sum())
    place = np.searchsorted(a.frame, frame)
    before, after = np.maximum(place - 1, 0), np.minimum(place, len(a) - 1)
    last = place == len(a)
    fits = ((place > 0) & (a.time[before] <= time) & (a_clock[before] <= clk)
            & (last | ((time <= a.time[after]) & (clk <= a_clock[after]))))
    out = np.zeros(len(candidates), bool)
    out[at[inside & fits]] = True
    return out, int((inside & ~fits).sum()), int((~inside).sum())


def gather(gaps, kind, issue, a, b, b_class, side_a, side_b, hi):
    """Add this security's rows from b that a lacks, as repair would: b's ONLY rows (classified against a's range extended to `hi`)
    that fit among a's."""
    only = b_class == ONLY
    if not only.any():
        return
    if issue not in gaps.recorded:
        gaps.left['not_recorded'] += int(only.sum())
        return
    restored = side_b.restored(b.seq)
    gaps.left['restored'] += int((only & restored).sum())
    if kind == 'trades':   # restored trades are in an order of their own: only the executions are neighbours
        a = a.only(~side_a.restored(a.seq))
    fits, misplaced, outside = fit(a, b, only & ~restored, side_a.clock[a.seq], side_b.clock[b.seq], side_a.lo, hi)
    gaps.left['misplaced'] += misplaced
    gaps.left['after_end'] += outside
    if fits.any():
        take = np.flatnonzero(fits)
        (gaps.quotes if kind == 'quotes' else gaps.trades)[issue] = (b.table.take(pa.array(take)), b.seq[take])


def scan(a, b, window, snapshots=True, until=None):
    """Compare recordings a and b (Sides): the Tallies of their quotes and trades, and the Gaps repair would fill in a (up to the
    source time `until` past a's end, if given). `snapshots`: whether rows that restate what came before a recording count."""
    tallies, gaps = {}, Gaps()
    hi = a.hi if until is None else max(a.hi, until)
    for kind, table, rows in (('quotes', DEPTHS, quotes), ('trades', TRADES, trades)):
        tallies[kind] = tally = Tally()
        for issue, a_table, b_table in paired(by_issue(a.directory / table), by_issue(b.directory / table)):
            x, y = rows(a_table, a), rows(b_table, b)
            if kind == 'quotes' and len(x):
                gaps.recorded.add(issue)
            a_class = classify(x, y, b.lo, b.hi, None if snapshots else a.restored(x.seq))
            b_class = classify(y, x, a.lo, hi, None if snapshots else b.restored(y.seq))
            tally.add(x, a_class, y, b_class, window)
            gather(gaps, kind, issue, x, y, b_class, a, b, hi)
    return tallies, gaps


def renumber(a, b, seqs):
    """How the batches go once the other's `seqs` are added to a's: (a's batches' numbers in the result, and the other's by their
    number in b). An added batch goes after the batches of a that are not later than it, never among the book and history that open
    a recording, and never after its end."""
    n = len(a.batches)
    seqs = np.array(seqs, np.int64)
    clocks = b.clock[seqs]
    place = np.clip(np.searchsorted(a.clock, clocks, 'right'), a.restoring, n - 1)
    rank = np.empty(len(seqs), np.int64)
    rank[np.lexsort((seqs, clocks, place))] = np.arange(len(seqs))
    kept = np.arange(n) + np.searchsorted(np.sort(place), np.arange(n), 'right')
    return kept, dict(zip(seqs.tolist(), (place + rank).tolist()))


class Writer:
    """A table written in row groups of ROW_GROUP rows, as the canonical encoding has them, from tables of any size."""

    def __init__(self, path, name):
        self.writer = pq.ParquetWriter(path, _parquet.SCHEMAS[name], **_parquet.options(name))
        self.pending, self.rows = [], 0

    def add(self, table):
        self.pending.append(table)
        self.rows += len(table)
        if self.rows >= ROW_GROUP:
            self._write(False)

    def _write(self, last):
        table = pa.concat_tables(self.pending)
        size = len(table) if last else len(table) // ROW_GROUP * ROW_GROUP
        for start in range(0, size, ROW_GROUP):
            self.writer.write_table(table.slice(start, min(ROW_GROUP, size - start)), row_group_size=ROW_GROUP)
        rest = table.slice(size)
        self.pending, self.rows = ([rest] if len(rest) else []), len(rest)

    def close(self):
        if self.pending:
            self._write(True)
        self.writer.close()


def with_issues(chunks, extra):
    """(issue, table) of a table's securities (`chunks`), and (issue, None) for those of `extra` it has no rows of, in order."""
    pending = sorted(extra)
    for issue, table in chunks:
        while pending and pending[0] < issue:
            yield pending.pop(0), None
        if pending and pending[0] == issue:
            pending.pop(0)
        yield issue, table
    for issue in pending:
        yield issue, None


def merged(table, seq, added, numbers):
    """A security's rows (`table`, or None for none, in batches `seq` already renumbered) with the other's `added` ((rows, their
    batches), or None) among them, the batches renumbered by `numbers` (indexed by the other's numbers), in batch order."""
    if table is None:
        table, seq = added[0], numbers[added[1]]
    elif added is not None:
        table = pa.concat_tables([table, added[0]])
        seq = np.concatenate([seq, numbers[added[1]]])
        order = np.argsort(seq, kind='stable')
        table, seq = table.take(pa.array(order)), seq[order]
    return table.set_column(table.schema.get_field_index('seq'), table.schema.field('seq'), pa.array(seq, pa.uint64()))


def write(a, b, gaps, output):
    """Write a with the rows of `gaps` added (from b) as a recording in `output`. Returns the number of batches added."""
    output = Path(output)
    (output / 'data' / 'custom').mkdir(parents=True, exist_ok=True)
    kept, added = renumber(a, b, gaps.batches())
    numbers = np.zeros(len(b.batches), np.int64)
    for seq, new in added.items():
        numbers[seq] = new
    for source, tables in ((DEPTHS, gaps.quotes), (TRADES, gaps.trades)):
        writer = Writer(output / source, source)
        try:
            for issue, table in with_issues(by_issue(a.directory / source), tables):
                writer.add(merged(table, None if table is None else kept[table.column('seq').to_numpy()], tables.get(issue), numbers))
        except BaseException:
            writer.writer.close()
            raise
        writer.close()
    batches = [None] * (len(a.batches) + len(added))
    for old, new in enumerate(kept.tolist()):
        batches[new] = {**a.batches[old], 'seq': new}
    for seq, new in added.items():
        original = b.batches[seq]
        require(original['type'] == 'quotes', "Only a frame's own batch can be added")
        batches[new] = {**{k: original[k] for k in ('type', 'source_time_us', 'received_unix_ms', 'decode_ns', 'replay_lateness_ms')},
                        'seq': new}
    end = batches[-1]
    end.update(frames=end['frames'] + len(added), quote_updates=end['quote_updates'] + sum(len(t) for t, _ in gaps.quotes.values()),
               source_time_us=max(end['source_time_us'], *(batches[new]['source_time_us'] for new in added.values()), 0))
    table = _parquet.Columns(None, None, None, [], batches, None, None)._batches(a.midnight)
    pq.write_table(table, output / _parquet.BATCHES, row_group_size=ROW_GROUP, **_parquet.options(_parquet.BATCHES))
    shutil.copyfile(a.directory / _parquet.EQUITY, output / _parquet.EQUITY)
    return len(added)


OVERLAP = 0.5   # recordings that share at least this much of the shorter one's time record the same stretch: they are feeds of it


def recordings_in(path):
    """The recordings a path names: the folder of a recording, or the folders in it that are (`brisk live --output`'s)."""
    path = Path(path)
    if (path / DEPTHS).is_file():
        return [path]
    found = sorted(p for p in path.iterdir() if (p / DEPTHS).is_file()) if path.is_dir() else []
    require(found, f'No recording in {path}')
    return found


def shared(x, y):
    """How much of the shorter one's stretch of the day both Sides cover, from 0 to 1."""
    span = min(x.hi - x.lo, y.hi - y.lo)
    return max(0, min(x.hi, y.hi) - max(x.lo, y.lo)) / span if span > 0 else 0.0


def plan(targets, sources, until=None):
    """[(target, partners, reach)]: for each Side to repair, the sources of its day that cover mostly the same time (its feed's
    other half, whichever broker or computer it is), and the last source time a repair may reach: where the next recording of the
    day starts, minus a microsecond, if one begins at or after the target's end, else its own end. Two recordings with different
    times, as a morning's and an afternoon's, are pieces one after the other, never repairs of each other, so a repair never
    goes into the next piece: that would store the day twice. `until` goes as far as it says, but not that far."""
    pool = {side.directory.resolve(): side for side in (*targets, *sources)}
    where = {side.directory.resolve() for side in sources}
    plans = []
    for target in targets:
        day = [side for key, side in pool.items() if side.date == target.date and key != target.directory.resolve()]
        partners = sorted((side for side in day if side.directory.resolve() in where and shared(target, side) >= OVERLAP),
                          key=lambda side: side.lo)
        later = [side.lo for side in day if shared(target, side) < OVERLAP and side.lo >= target.hi]
        limit = min(later) - 1 if later else None
        if until is not None and limit is not None and until > limit:
            raise ValueError(f'The recording that follows starts at {clock(limit + 1)}: a repair that went past it would store '
                             'that time twice')
        reach = limit if until is None else until
        plans.append((target, partners, reach if reach is not None and reach > target.hi else None))
    return plans


def compare(recording, other, window=5.0, snapshots=True):
    """How two recordings of one day relate: rows by class and blocks by status, as `repair` reports it of each source."""
    a, b = _sides(recording, other)
    return {'recording': a.describe(), **_report(a, b, window, snapshots)[0]}


def _sides(recording, other):
    a, b = Side(recording), Side(other)
    require(a.date == b.date, f'The recordings are of different days ({a.date} and {b.date})')
    return a, b


def _report(a, b, window, snapshots, until=None):
    """(how b relates to a, the Gaps of repairing a from b)."""
    tallies, gaps = scan(a, b, int(window * 60 * 10**6), snapshots, until)
    return {'other': b.describe(), 'window_minutes': window, 'snapshots': snapshots,
            'quotes': tallies['quotes'].report(), 'trades': tallies['trades'].report()}, gaps


def _refusal(theirs):
    return (f"The recordings do not number their frames alike: of the other's updates {theirs['same']:,} are the same as "
            f"this one's and {theirs['conflict']:,} have a frame number here with other content, so what one lacks "
            'cannot be told. Nothing is written (brisk repair without --output counts them)')


def _run(target, partners, reach, window, deliver, work):
    """Repair a Side from each of its partners in turn (each pass writes in `work`, and the next starts from it): returns what
    was found and done. `deliver(target, tables, entry)` receives the finished tables, if any."""
    entry = {'recording': {**target.describe(), 'path': str(target.directory)}, 'sources': [],
             'reach': None if reach is None else clock(reach)}
    current, passes = target, 0
    for partner in partners:
        comparison, gaps = _report(current, partner, window, True, reach)
        theirs = comparison['quotes']['rows']['b']
        # What one lacks can be told only if the two number their frames alike: then most of the other's updates that this has a
        # frame of are the same, not other content under the same number.
        match = theirs['same'] > theirs['conflict'] or not (theirs['same'] or theirs['conflict'] or theirs['only'])
        comparison['repair'] = {'quotes': sum(len(t) for t, _ in gaps.quotes.values()),
                                'trades': sum(len(t) for t, _ in gaps.trades.values()), 'batches': len(gaps.batches()),
                                'securities': len(set(gaps.quotes) | set(gaps.trades)), 'left_out': gaps.left,
                                'frame_numbers_match': match}
        entry['sources'].append({'path': str(partner.directory), **comparison})
        if deliver is not None and match and gaps.batches():
            passes += 1
            write(current, partner, gaps, work / str(passes))
            current = Side(work / str(passes))
    refused = [source for source in entry['sources'] if not source['repair']['frame_numbers_match']]
    if deliver is not None and entry['sources'] and len(refused) == len(entry['sources']):
        raise ValueError(_refusal(refused[0]['quotes']['rows']['b']))
    entry['written'] = current is not target
    if entry['written']:
        deliver(target, current.directory, entry)
    return entry


def repair(recordings, others, deliver=None, until=None, window=5.0, work=None):
    """Repair recordings from other feeds' recordings of the same day, adding what only they hold. `recordings` and `others` are
    folders of recordings, or of folders of them (a `brisk live --output` folder): a recording is repaired from each of the others
    that covers mostly the same time (OVERLAP; see `plan`), adding its quote updates and trades the recording lacks, each frame as a
    batch of its own with the other's receipt time, among its own. Nothing is changed or removed: where two disagree, the recording
    stands. Only frames within the recording's own range of source time are added, and, where another recording follows it, those
    up to where that begins (closing the gap a reconnect left); `until` (a time of day, for one recording) goes further where
    nothing follows, as when a feed stopped early. Of a pair that does not number its frames alike nothing is added.

    `deliver(recording, tables, entry)` is called with the folder holding each repaired recording's tables, which are removed after
    (written under `work`, a folder on the output's disk); with none, nothing is written and the report says what would be.
    Returns {'recordings': [...]}: for each, its range, how far a repair may reach, and for each source the comparison (rows by
    class, blocks by status) and what was added: quotes, trades, batches, securities, and what was left out and why (`misplaced`: the
    frame does not fit the order of the recording's own rows around it; `after_end`: past its range; `restored`: only in the other's
    opening book or restored trades, which carry no receipt time; `not_recorded`: of securities the recording does not have)."""
    loaded = {}

    def side(path):
        key = Path(path).resolve()
        if key not in loaded:
            loaded[key] = Side(path)
        return loaded[key]
    targets = list({side(d).directory.resolve(): side(d) for path in recordings for d in recordings_in(path)}.values())
    sources = [side(d) for path in others for d in recordings_in(path)]
    require(until is None or len(targets) == 1, '--until is for one recording at a time')
    until = None if until is None else micros(targets[0].date, until)
    with tempfile.TemporaryDirectory(dir=work) as tmp:
        entries = [_run(*job, window, deliver, Path(tmp) / str(n)) for n, job in enumerate(plan(targets, sources, until))]
    require(any(entry['sources'] for entry in entries),
            'Nothing to repair from: none of the others is of the same day and covers mostly the same time as a recording')
    return {'recordings': entries}
