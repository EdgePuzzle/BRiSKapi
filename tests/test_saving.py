"""What `brisk live` tells on stderr of a recording it saves (briskapi.cli.Saving), with a clock the test winds."""
import io

import pytest

from briskapi import cli

LABEL = 'sbi 2026-03-11 from 09:00:00'


class Clock:
    def __init__(self):
        self.now = 0

    def __call__(self):
        return self.now


def saving(label=LABEL, every=30):
    clock, out = Clock(), io.StringIO()
    return cli.Saving(label, every, out, clock), clock, out


def test_a_save_tells_each_stage_at_once_and_its_rows_now_and_then():
    save, clock, out = saving()
    save.packaging('package: preparing')
    save.packaging('package: batches 10/100')
    clock.now = 1
    save.packaging('package: batches 20/100')   # too soon after the last line
    clock.now = 31
    save.packaging('package: batches 50/100')
    save.packaging('package: quotes 1000/4000')   # a stage's first line is never held back
    save.packaging('package: trades 5/20')
    clock.now = 70
    save.packaging('package: quotes 3000/4000')
    save.packaging('package: finalizing')
    save.saved('/days/sbi_20260311_090000-093000')
    assert out.getvalue().splitlines() == [
        f'[brisk] {LABEL}: preparing to save, 0s elapsed',
        f'[brisk] {LABEL}: writing batches 10 of 100 (10%), 0s elapsed',
        f'[brisk] {LABEL}: writing batches 50 of 100 (50%), 31s elapsed',
        f'[brisk] {LABEL}: writing quotes 1,000 of 4,000 (25%), 31s elapsed',
        f'[brisk] {LABEL}: writing quotes 3,000 of 4,000 (75%), trades 5 of 20 (25%), 1m10s elapsed',
        f'[brisk] {LABEL}: finalizing, 1m10s elapsed',
        f'[brisk] {LABEL}: saved to /days/sbi_20260311_090000-093000 in 1m10s',
    ]


def test_nothing_is_said_unless_asked_and_a_label_nobody_needed_is_never_made():
    made = []
    save, clock, out = saving(label=lambda: made.append(1) or LABEL, every=None)
    for label in ('package: preparing', 'package: quotes 1/2', 'package: finalizing'):
        save.packaging(label)
    save.saved('/days/x'); save.uploading('uploading', 1, 2); save.uploaded(); save.failed('Packaging', 'no')
    assert out.getvalue() == '' and made == []
    save, clock, out = saving(label=lambda: made.append(1) or LABEL)
    save.packaging('verify: batches')   # not a package's line: nothing to tell
    save.packaging('package: preparing'); save.packaging('package: finalizing')
    assert made == [1] and out.getvalue().count(LABEL) == 2


def test_a_label_that_cannot_be_made_does_not_lose_the_line():
    def broken():
        raise OSError('the first part is gone')
    save, clock, out = saving(label=broken)
    save.packaging('package: preparing')
    assert out.getvalue() == '[brisk] recording: preparing to save, 0s elapsed\n'


def test_a_stderr_that_fails_does_not_fail_the_save():
    class Closed:
        def write(self, text):
            raise BrokenPipeError

        def flush(self):
            raise BrokenPipeError
    save = cli.Saving(LABEL, 30, Closed(), Clock())
    save.packaging('package: preparing')
    save.saved('/days/x')
    save = cli.Saving(LABEL, 30, io.StringIO(), Clock())
    save._out.close()
    save.packaging('package: preparing')   # ValueError: I/O operation on closed file


def test_a_total_not_known_has_no_percentage():
    assert cli._of(5, 20) == '5 of 20 (25%)' and cli._of(5, 0) == '5' and cli._of(2**20, 3 * 2**20, cli._mib) == '1.0 MiB of 3.0 MiB (33%)'


@pytest.mark.parametrize('seconds,text', [(0, '0s'), (59.9, '59s'), (60, '1m00s'), (251, '4m11s'), (3723, '1h02m03s')])
def test_a_duration_reads_as_a_person_says_it(seconds, text):
    assert cli._duration(seconds) == text


def test_the_upload_is_told_apart_and_timed_from_its_own_start():
    save, clock, out = saving(every=30)
    save.packaging('package: preparing')
    clock.now = 100
    save.saved('/days/x')
    clock.now = 110
    save.uploading('preparing')
    save.uploading('uploading', 0, 3 * 2**20)
    clock.now = 115
    save.uploading('uploading', 2**20, 3 * 2**20)   # too soon after the last line
    clock.now = 150
    save.uploading('uploading', 3 * 2**20, 3 * 2**20)
    clock.now = 151
    save.uploading('validating')
    clock.now = 190
    save.uploaded()
    assert out.getvalue().splitlines()[2:] == [
        f'[brisk] {LABEL}: preparing to upload, 0s elapsed',
        f'[brisk] {LABEL}: uploading 0.0 MiB of 3.0 MiB (0%), 0s elapsed',
        f'[brisk] {LABEL}: uploading 3.0 MiB of 3.0 MiB (100%), 40s elapsed',
        f'[brisk] {LABEL}: sent; waiting for the archive to check it, 41s elapsed',
        f'[brisk] {LABEL}: uploaded; the archive stored it (1m20s)',
    ]


def test_a_failure_says_where_it_stopped_and_how_long_it_took():
    save, clock, out = saving()
    save.packaging('package: preparing')
    clock.now = 190
    save.failed('Packaging', ValueError('Recording has no clean end'))
    assert out.getvalue().splitlines()[-1] == f'[brisk] {LABEL}: Packaging failed after 3m10s: Recording has no clean end'


def test_a_stage_without_rows_and_a_hint_after_where_it_is_saved():
    save, clock, out = saving()
    save.stage('packaging')
    save.stage('packaging')   # a stage is told once, as it begins
    clock.now = 4
    save.saved('/days/x', '`brisk upload /days/x` retries the upload')
    assert out.getvalue().splitlines() == [f'[brisk] {LABEL}: packaging, 0s elapsed',
                                           f'[brisk] {LABEL}: saved to /days/x in 4s; `brisk upload /days/x` retries the upload']


def test_a_save_kept_instead_is_told_afresh_and_timed_from_there():
    save, clock, out = saving()
    save.packaging('package: preparing')
    clock.now = 190
    save.failed('Packaging', 'no')
    clock.now = 191
    save.keeping('/data/recordings')
    save.packaging('package: preparing')   # a stage told before is told again: this is another attempt
    clock.now = 200
    save.packaging('package: quotes 5/10')
    clock.now = 261
    save.saved('/data/recordings/x')
    assert out.getvalue().splitlines()[2:] == [
        f'[brisk] {LABEL}: keeping what was recorded in /data/recordings, 0s elapsed',
        f'[brisk] {LABEL}: preparing to save, 0s elapsed',
        f'[brisk] {LABEL}: writing quotes 5 of 10 (50%), 9s elapsed',
        f'[brisk] {LABEL}: saved to /data/recordings/x in 1m10s']


def test_saves_are_quiet_unless_brisk_live_runs(monkeypatch):
    assert cli.SAVE_EVERY is None and cli.saving(LABEL)._every is None
    monkeypatch.setattr(cli, 'SAVE_EVERY', 5)
    assert cli.saving(LABEL)._every == 5


def test_a_piece_the_stream_sent_no_update_to_is_told_to_have_nothing_to_save():
    save, clock, out = saving('sbi 2026-03-11 from 11:45:00')
    save.empty('12:00:03')
    assert out.getvalue() == '[brisk] sbi 2026-03-11 from 11:45:00: no update came until 12:00:03; nothing to save\n'
