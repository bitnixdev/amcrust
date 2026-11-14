# Matched native acquisition resource observations

The [continuous native fanout probe](native-fanout-proof.md) established lifecycle and
ownership behavior. `scripts/native_resource_probe.py` now compares two **experimental**
acquisition arrangements with matched media and consumer work:

- **Shared:** one native RTSP/IPC worker and one normalizer/admission gate, feeding N
  independently leased consumers.
- **Independent:** N native RTSP/IPC workers and N normalizer/admission gates, each
  feeding one consumer. All acquire from the same synthetic fixture endpoint.

This is **not a comparison against the production bridge**: production FFmpeg output,
audio transcoding, RTP/SRTP, HomeKit recording transport and Apple clients are absent.
No production process/settings or physical cameras were changed/accessed.

## Matched work and measurement boundaries

The fixture uses Main H.264, 320×180 at 15 fps, mono 48 kHz AAC, a 250 ms audio offset,
and staggered sender reports. Each trial starts fresh publisher and fixture Python
processes. The fixture generates finite input media **before** its ready signal and
before measured CPU intervals. Encoding/input-generation CPU is excluded.

For each duration, every consumer must receive all normalized packets from sequence
zero. Per-source complete-wire hashes check payload, timestamps and side-data delivery.
A second generation-independent fingerprint covers the codec descriptors/shared epoch
and packet sequence, track, PTS, DTS, duration, flags and payload bytes. It excludes
per-packet transport side data; the complete-wire oracle still covers those within each
source. Every acquisition and consumer within a trial must have the same fingerprint.
All twelve six-second trials shared one fingerprint; both twenty-second trials shared
another. The fingerprints and unrounded observations are in
[`native-resource-results.json`](native-resource-results.json).

The fixture previously aggregated packet counters across clients when writing SRs.
Its RTCP packet/octet counters are now connection-local, while diagnostic totals remain
aggregate. A regression checks that a second client starts with zero SR counters.

### CPU

- Opt-in native `--rtsp-resource-fixture` uses the same loopback-only acquisition path
  and emits one `getrusage(RUSAGE_SELF)` report at normal C exit. Native CPU is summed
  user + system time across workers, including startup, stream discovery, demux and IPC
  serialization through the report—not a demux-only or steady-state measurement.
- Native per-process lifetime peak RSS is also reported, with Darwin bytes and Linux
  KiB explicitly normalized. Individual high-water marks are **not summed** to pretend
  they occurred simultaneously. Killed/crashed/malformed/missing reports are not credited.
- Publisher CPU uses `process_time()` from acquisition launch through worker cleanup and
  sampler completion. It includes normalization, inspection/re-encoding, publication,
  queues, consumers, oracle hashes and sampler coordination. It excludes native/fixture
  processes and the external `ps` processes' CPU. This is not total machine CPU.
- Fixture CPU is separately measured after input generation through server shutdown.
  Its RTP/TCP work scales with the number of connections; it is not real camera CPU.
- Additional process-CPU windows surround normalization, publication, oracle hashing,
  and consumer hashing/release. These are **subsets of publisher CPU**, not additive
  totals on top of it. They omit receive/dispatch and other work, include instrumentation
  overhead, and can include incidental background-thread CPU during those windows.

### RSS

The sampler requests only PID and RSS for its publisher, fixture and currently running
native children—no arguments, credentials, environments or broad process enumeration.
It waits 250 ms after each completed `ps` snapshot; process/snapshot time adds to that,
so this is **not a fixed 4 Hz sampler**. Six-second trials obtained 16–17 snapshots that
covered all workers; twenty-second trials obtained 56–57.

Reported worker RSS is the largest observed sum of rows in one snapshot. Sampling can
miss peaks; a snapshot is not perfectly simultaneous; shared library pages can be
counted multiple times. This is neither unique/private physical memory, PSS, reclaimed
memory, nor a true aggregate peak. Publisher and fixture RSS are reported separately.
The fixture retains generated input and event schedules, whose size grows with duration.

The Darwin Nix build sandbox does not permit `ps`. Nix smoke checks explicitly pass
`--no-rss`, reporting `enabled: false` without invented zero values. All recorded trials
below used RSS sampling enabled outside the build sandbox. There is no automatic
fallback when sampling fails. Linux gets `procps` through the repository derivation;
Darwin uses its OS-provided `/bin/ps`.

## Six-second observations

Darwin arm64, Python **3.14.10**, repository-pinned FFmpeg **9.0.2**. Two sequential trials
per arrangement, with reversed case order on the second pass. Ranges below are the two
observations, not confidence intervals. Input: **90 video + 270 AAC packets per source**;
each consumer received 360 matching packets.

| Consumers | Mode | Workers/connections | Native CPU s | Publisher CPU s | Fixture CPU s | Largest sampled worker RSS sum, MiB |
|---:|---|---:|---:|---:|---:|---:|
| 1 | Shared | 1 | 0.0689–0.0717 | 0.2713–0.2915 | 0.0372–0.0402 | 17.34–17.39 |
| 1 | Independent | 1 | 0.0680–0.0702 | 0.2538–0.2850 | 0.0326–0.0373 | 17.47–17.55 |
| 2 | Shared | 1 | 0.0694–0.0702 | 0.2889–0.2951 | 0.0374–0.0378 | 17.31–17.39 |
| 2 | Independent | 2 | 0.1354–0.1356 | 0.4019–0.4127 | 0.0457–0.0461 | 34.98–35.55 |
| 8 | Shared | 1 | 0.0693–0.0700 | 0.3389–0.3459 | 0.0366–0.0374 | 17.42–17.75 |
| 8 | Independent | 8 | 0.5269–0.5287 | 1.1050–1.1103 | 0.1462–0.1491 | 141.41–141.45 |

At eight consumers, normalization windows were 0.0660–0.0669 s shared versus
0.3745–0.3760 s independent; publication windows were 0.0994–0.1014 versus
0.4252–0.4285 s. Consumer hash/release windows remained 0.0193–0.0196 versus
0.0220–0.0221 s. This distinguishes duplicated source work from the still-independent
consumer work without pretending the windows account for all publisher CPU.

## Twenty-second extension

One trial per arrangement, eight consumers. Each source published **300 video + 926
AAC packets**; each arrangement delivered **9,808 consumer packets** with matching
workload fingerprints and exact per-source delivery hashes.

| Mode | Native CPU s | Publisher CPU s | Fixture CPU s | Sampled worker RSS sum, MiB | Publisher / fixture sampled RSS, MiB |
|---|---:|---:|---:|---:|---:|
| Shared | 0.1228 | 1.1417 | 0.1135 | 17.56 | 33.92 / 34.95 |
| Independent | 0.8290 | 3.7018 | 0.4900 | 141.45 | 34.44 / 36.22 |

The longer cases did not show a large increase in sampled publisher/worker RSS, but
one twenty-second observation is not a leak theorem, production soak, or long-term
memory bound. Startup is included, rates are paced, and there is no saturation or
first-frame-latency measurement. These observations support avoiding duplicated native
acquisition for this workload; they do **not** predict production CPU/RSS savings or
performance at camera resolutions, other codecs, networks, or subscriber behaviors.

## Reproduction and remaining gates

```sh
nix build .#ingest-worker-probe --no-link --print-out-paths
# Use the matching worker in the derivation's development environment:
nix develop .#ingest-worker-probe --command python3 scripts/native_resource_probe.py \
  --worker /path/to/ingest-worker-probe --mode shared --subscribers 8 --seconds 6
# Repeat with --mode independent; limits are 1–8 consumers and 3–20 seconds.
```

Every complete trial requires exact packet counts, delivery/workload hashes, expected
connection counts, native reports and worker cleanup. Failure/cancellation closes
subscriber state before reaping native children. The input fixture has an isolated
process group so startup failure also stops input-generation children, even if the
fixture leader has already failed. Unit checks cover report/units parsing, missing
measurements, fingerprint semantics, cancellation cleanup and connection-local SRs.

Next correctness work: late AAC/preroll and recording-start/mux contracts, preserving
source clocks and direct-copy controls rather than hiding gaps or trimming unexplained
samples. Broader clock/codec representations, source registry/privacy/backoff, both
real cameras and Apple live/recording acceptance remain open. These scoped resource
observations do not authorize production integration or close milestone 0's hardware
and media-semantics gates.
