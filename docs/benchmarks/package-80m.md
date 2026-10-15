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
measure real-market compression, a running feed, or total memory on a 24 GiB M4;
trades and changing books still need testing with the tester's retained spools.

The automated regression
`test_a_broker_kept_without_uploads_splits_before_lunch_or_the_streams_end`
also checks the old and new thresholds at reduced scale. Its host cannot finish
until a piece has been saved, so a successful result demonstrates a size cut while
the feed is still running, with uploads off and an output directory specified.
