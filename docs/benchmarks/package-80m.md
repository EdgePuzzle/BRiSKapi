# Packaging at the 80-million-row threshold

Measured locally on 2026-10-15, on macOS with 48 GiB of memory and 15 logical CPUs.
The recorder's size threshold is now 80 million quote rows; the lunch and day-end
cuts still apply. A completed spool part can take a piece just past the threshold,
and a following piece repeats its opening book. The archive allows 100 million
depth rows per recording.

The input was self-authored synthetic data: 4,000 securities, 80 million quote
updates in 1.6 million batches, ten levels on each side of the book, and no trades.
An opening snapshot added 4,000 quote rows in its own part; another 1,600 parts
held 50,000 updates each, grouped by 32 issue IDs as the recorder writes them.
Clocks, frame numbers and batch numbers advanced across parts. Books repeated,
so this input compresses much more than real market data.

Two `brisk-parquet package` processes read the same spool concurrently, with three
threads each, the recorder's default on a ten-core M4. Each repaired the final end
marker and produced 80,004,000 depth rows and 1,600,002 batch rows.

| Packaging job | Elapsed seconds | Peak process RSS, GiB |
| --- | ---: | ---: |
| A | 155.60 | 3.865 |
| B | 154.81 | 3.811 |

An earlier single package of the same input also passed the Rust archive verifier.
The two concurrent outputs had the same sizes (39,789,420 bytes of Parquet tables).
These runs check the larger row count and concurrent packaging. They do not
measure real-market compression or a running feed; the section below does, on
real sessions.

The automated regression
`test_a_broker_kept_without_uploads_splits_before_lunch_or_the_streams_end`
also checks the old and new thresholds at reduced scale. Its host cannot finish
until a piece has been saved, so a successful result demonstrates a size cut while
the feed is still running, with uploads off and an output directory specified.

## Real sessions on a 24 GB M4

Measured by a tester and reported to the maintainers, not run by them. The machine was
an Apple M4 (10 cores, 24 GB) with Python 3.14.8 (free-threaded) and Node 22.23.3, running
briskapi 1.6.1 from its PyPI wheel (its own arm64 `brisk-parquet`), with uploads off. The
input was four spools of broker sessions recorded on 15 October 2026 that the feed had
left behind (two morning sessions and two afternoon sessions, 17 GB together, from two
broker feeds). They were copied back into the spool one at a time, originals kept, and
recovered by `brisk live --no-upload`, which repairs the end each spool never got and
packages it with `brisk-parquet package` on three threads (the recorder's default: a third
of the cores). All four were saved and read back.

| Session | Quote rows in the spool | Elapsed | Peak RSS | Recordings made |
| --- | ---: | ---: | ---: | --- |
| Afternoon, 12:35 to 14:36 | 27,251,337 | about 55 s | 4.7 GB | one, 696 MB |
| Afternoon, 12:35 to 15:35 | 41,214,585 | about 95 s | 5.2 GB | one, 1,038 MB |
| Morning, 08:01 to 11:32 | 63,244,759 | about 130 s | 5.9 GB | 60,042,765 rows (1,454 MB) and 3,206,437 rows (84 MB) |
| Morning, 08:01 to 11:33 | 62,413,523 | 111.6 s and 3.5 s | 5.60 GB and 2.99 GB | 60,010,287 rows and 2,407,679 rows |

- Peak RSS was sampled with `ps` every 5 s (10 to 25 s under load). The 60-million-row piece
  of the last session was measured exactly, with `/usr/bin/time -l` between the recorder and
  the program: 111.6 s, 288 s of CPU, 5.60 GB. Swap stayed at 2.4 GB and did not grow. The
  Python process stayed below 0.2 GB while packaging and held 2.0 to 2.8 GB idle afterwards.
- A session of more than 60 million rows is cut in two, as designed. The second piece repeats
  each issue's last quote (4,443 rows) as its bootstrap, and the two pieces' rows add up to
  the parts' rows plus 4,443.
- On disk a real session is about 24 to 26 bytes a quote row (696 MB for 27.3 million rows,
  1,038 MB for 41.2 million, 1,454 MB for 60.0 million). The synthetic input above is about
  0.5, because its books repeat. The 60-million-row real piece used 5.6 GB at about 540,000
  rows a second; the 80-million-row synthetic jobs used 3.9 GiB at about 520,000 rows a second
  (two at once, on another machine). The tester reported GB and the table above is in GiB,
  which could account for up to 7% of the difference; the rest is real data needing noticeably
  more memory than the synthetic check suggested. It still fits in 24 GB.
- Every recording read back through `briskapi.Recording` with 4,443 issues in its final state.
  The 7203 updates matched the spool parts exactly (same sequence and values, no mismatches;
  the one update at a piece boundary is in both pieces), and trades read back too.
