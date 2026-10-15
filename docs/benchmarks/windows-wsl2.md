# Native Windows versus WSL2 — 14 October 2026

WSL2 did not deliver a large throughput improvement on this i5-12600K. Its best measured full-pipeline result was 41,828 quote updates/s, versus 45,143 on unrestricted native Windows. A processor-count-limited Windows control showed a modest WSL advantage, not the roughly fivefold difference from the earlier M5 Pro repository result.

## Setup

- Same i5-12600K host and source commit `4267568eac78eba85bef0c3c06b7b34ec7913a4f`.
- Ubuntu under WSL2, kernel `6.18.54.1-microsoft-standard-WSL2`, glibc 2.39.
- WSL is configured for **8 virtual CPUs**, versus 16 available logical processors in unrestricted native Windows. Its memory limit is approximately 24 GB; native Windows has approximately 48 GB.
- Code, fixture files, environment, spool and recordings were placed in a temporary Linux working directory on WSL's **ext4** filesystem. The timed work did not use `/mnt/c`.
- Both environments used Node 25.8.0, free-threaded Python 3.14.8, NumPy 2.5.4, PyArrow 26.0.0 and pandas 3.0.6. The GIL remained off after imports.
- Python version numbers match, but the builds differ: native Windows uses the python.org MSVC build; WSL uses uv's Clang-built distribution with tail-call interpreter, PGO, LTO and BOLT enabled.
- Benchmarks ran sequentially, with the same copied harness and pinned historical demo assets. All uploads were disabled; no real broker authentication or remote market feed was used.

## Full replay throughput

Each run applied 731,251 quote updates from 18,001 wire frames through the actual live decoder host, its quote workers, JSON pipe, Python Feed, recording spool and enabled local gateway. No Live consumer was attached for this table. Every recording completed successfully with the expected update count.

| Environment | Processor availability | 4 workers | 8 workers |
|---|---|---:|---:|
| Native Windows, original measurement | 16 logical processors | 43,531/s | 45,143/s |
| Native Windows, new count-limited control | 8 logical processors, affinity `0xff` | 36,556/s | 30,291/s |
| Ubuntu WSL2 | 8 virtual CPUs | 41,828/s | 37,397/s |

The count-limited Windows control set affinity only for the benchmark process and its children; Node reported eight available processors. Host-wide settings were unchanged. The same processor count is not necessarily the same physical-core/SMT assignment: Windows affinity selects logical processors 0–7, whereas WSL's virtual processors are scheduled by the hypervisor. This is a useful control, not a pure isolation of the OS.

Against the count-limited Windows control, WSL throughput was approximately 14% higher with four workers and 23% higher with eight. With each environment's normal CPU allocation, native Windows's best result was about 8% higher than WSL's best. These are single measurements per configuration, not statistical estimates of a fixed platform penalty.

Four workers is the automatic quote-worker budget under this WSL configuration (`availableParallelism() - 4`). Eight workers reduced WSL throughput and used more memory.

Final recording assembly, measured separately from stream application, took 15.786 s on WSL with four workers, compared with 23.999 s on unrestricted Windows and 21.122 s on count-limited Windows. Stream throughput above includes concurrent spool writes but excludes this final assembly after the feed ends.

## Decoder-only control

The existing benchmark, with no JSON pipe, Python feed or recording:

| Environment | Replay elapsed | Quote updates/s |
|---|---:|---:|
| Native Windows, original measurement | 14.330 s | approximately 51,000 |
| Ubuntu WSL2 | 16.071 s | approximately 45,500 |

This did not reveal a Windows-specific slowdown in the standalone decoder.

## Two-symbol Live callback latency

Both clients subscribed to `mbp-10` for 7203 and 6758 and were ready on the fast lane before streaming began. Recording and the gateway remained enabled. The sample covered the first 600 wire frames / 5.96 source seconds, with 65,250 quote updates plus the initial snapshot.

| Replay | Native Windows default: 8 workers | WSL default: 4 workers |
|---|---:|---:|
| Original pace, callback p50 | 10.48 ms | 7.19 ms |
| Original pace, callback p99 | 67.73 ms | 116.53 ms |
| 12× requested pace, callback p99 | 1.627 s | 1.145 s |

The original-pace sample finished on schedule on both. Both developed backlog under accelerated input. Latency results are mixed rather than a uniform improvement from changing platforms.

For completeness, an eight-worker WSL full-replay stress run at 32× requested pace applied all 731,251 updates, with all 4,276 expected post-subscription two-symbol callbacks, but showed 29.306 s p99 callback lag. That oversubscribed configuration is not WSL's default; its raw result is retained in `benchmark-wsl-stress.json`. The compressed stress workload is not an exact model of an actual opening's arrival pattern.

Latencies measure same-machine Node receipt to state/callback using wall clocks at millisecond receipt precision. They are not exchange latency. Unpaced latency distributions are not a fair standalone platform comparison because asynchronous output can let the producer enqueue much more data before the consumer catches up.

## Interpretation

The [documented Windows/POSIX difference in Node stdout pipe behavior](https://nodejs.org/api/process.html#a-note-on-process-io) did not translate into a severalfold WSL throughput gain in this workload. The evidence weakens the hypothesis that Windows alone accounts for the earlier M5 gap. It does not prove that hardware alone explains it: VM CPU limits, core placement, runtime/compiler builds, allocation and thread scaling, and the unmatched historical M5 measurement remain confounders.

The most useful next comparison is the **identical harness and current source revision on the M5 Pro**, followed by phase profiling of JSON parsing, state application and concurrent recording. Moving this workload to the current WSL configuration does not reproduce the repository's approximately 236,000 updates/s M5 result.

## Reproduction

The self-authored harness is `tools/brisk_mock/benchmark_feed.py`, with its fake broker host in `benchmark_feed.cjs`. Only aggregate measurements are committed in [windows-wsl2/](windows-wsl2/); vendor assets and market recordings are excluded. The initial runs used a local copy of this harness; it has since been moved to these portable paths with a configurable fixture location.

Create a free-threaded Python environment and install the development requirements as described in [CONTRIBUTING.md](../../CONTRIBUTING.md). Use Node 25.8.0 and the dependency versions above to match these results. Download the pinned demo assets once:

```sh
python tools/brisk_mock/download_mock.py --cache cache/brisk-mock
node tools/brisk_mock/benchmark.cjs cache/brisk-mock
python tools/brisk_mock/benchmark_feed.py --cache cache/brisk-mock --workers 4 8 --frames 18001 --output cache/full.json
python tools/brisk_mock/benchmark_feed.py --cache cache/brisk-mock --workers 4 --speeds 1 12 --fast-symbols 7203 6758 --output cache/latency.json
python tools/brisk_mock/benchmark_feed.py --cache cache/brisk-mock --workers 8 --frames 18001 --speed 32 --fast-symbols 7203 6758 --output cache/stress.json
```

Run with the environment's Python (`.venv/bin/python` on Linux/macOS, `.\.venv\Scripts\python.exe` on Windows). For WSL, copy the checkout and fixtures to its Linux filesystem, create a Linux environment there, and run the same commands inside Ubuntu. Avoid timing against files on `/mnt/c`. Match worker settings when comparing platforms, and repeat runs before treating small differences as significant.

On native Windows, the processor-count control is:

```powershell
.\.venv\Scripts\python.exe tools/brisk_mock/benchmark_windows_affinity.py --workers 4 8 --frames 18001 --output cache/eight-cpus.json
```

That wrapper restricts only its own process and children to logical processors 0–7; it does not change host or WSL configuration.

### Measurement files

- [benchmark-decoder.json](windows-wsl2/benchmark-decoder.json)
- [benchmark-feed-full.json](windows-wsl2/benchmark-feed-full.json)
- [benchmark-fast-warmed.json](windows-wsl2/benchmark-fast-warmed.json)
- [benchmark-windows-eight-cpus.json](windows-wsl2/benchmark-windows-eight-cpus.json)
- [benchmark-wsl-decoder.json](windows-wsl2/benchmark-wsl-decoder.json)
- [benchmark-wsl-full.json](windows-wsl2/benchmark-wsl-full.json)
- [benchmark-wsl-default-fast.json](windows-wsl2/benchmark-wsl-default-fast.json)
- [benchmark-wsl-fast.json](windows-wsl2/benchmark-wsl-fast.json)
- [benchmark-wsl-stress.json](windows-wsl2/benchmark-wsl-stress.json)
