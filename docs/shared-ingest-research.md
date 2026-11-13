# Shared ingest: baseline and first backend experiment

Status: **milestone 0 partially complete**. Production acquisition is unchanged.
The synthetic CLI and [packet-aware native-worker experiment](packet-ingest-proof.md)
are complete for local files; RTSP, output-playback and real-camera gates remain
open. This document records reproducible evidence, not a performance claim about
deployed cameras. The local living plan is intentionally not tracked.

## Production baseline instrumentation

New fixed-label Prometheus metrics:

- `amcrust_live_first_packet_seconds_{count,sum}{packet="rtp"|"idr"}`:
  elapsed time from video proxy construction to the first successfully sent video
  RTP packet / first packet containing an H.264 IDR start. These are aggregate
  summaries without quantiles. They **do not** measure a complete keyframe or
  controller decode, and samples only exist for sessions reaching that point.
  Compare with `amcrust_live_stream_starts_total`; failed starts must not disappear
  from the evaluation just because they produced no latency sample.
- `amcrust_recording_deliveries_total{outcome=...}`: pump outcomes `end_sent`,
  `cancelled`, `source_unavailable`, `write_error`. `source_unavailable` includes
  missing initialization, source timeout, closed channel and receiver lag.
  `end_sent` means the end-of-stream fragment was written, **not** that iCloud
  acknowledged/stored the clip. Rejected opens and still-running pumps are not
  counted. This separates delivery outcomes from generated fragments/bytes.

Read-only process-tree sampling (run on the host running the service):

```sh
python3 -B scripts/ingest_baseline.py --pid <amcrust-pid> \
  --metrics-url http://127.0.0.1:<metrics-port>/metrics --samples 30 \
  > /tmp/amcrust-baseline.jsonl
```

The sampler collects process count, FFmpeg count, summed RSS, OS-reported `ps` CPU
percentages, and optional metrics. It reads executable names, **not command-line
arguments or environment variables**, and does not touch cameras. Metrics URLs
must be local, credential-free HTTP; redirects and proxies are disabled.

Caveats: summed RSS can double-count shared memory; `ps` CPU percentages may be
lifetime averages rather than interval usage. Process snapshots are racy during
startup/shutdown. FFmpeg process counts are **not** established RTSP connection
counts. Use camera session diagnostics or a narrowly scoped socket capture for the
latter, without retaining credentials/media. The sampler is not a benchmarking
replacement for platform CPU profilers.

## Reproduce the CLI backend experiment

Requires Python 3.9+ and FFmpeg/ffprobe with libx264 and AAC. No Python packages,
camera credentials, network access, or changes to camera settings are needed.
Use repository-managed Nix tooling when adding unavailable dependencies; do not
install them system-wide.

```sh
python3 -B -m unittest discover -s scripts -p 'test_*.py' -v
python3 -B scripts/ingest_probe.py --seconds 3 > /tmp/ingest-no-bframes.json
python3 -B scripts/ingest_probe.py --seconds 3 --bframes 2 > /tmp/ingest-bframes.json
```

The probe:

1. Generates a temporary 640×360/15 fps H.264 + mono 48 kHz AAC MP4 fixture.
2. Uses **one** paced FFmpeg acquisition/remux process (`-c copy`) producing NUT.
3. Fans its output into two separately queued file consumers and a deliberately
   stalled subscriber. No consumer is awaited by the producer.
4. Detaches the stalled subscriber on overflow, clearing its queue. Healthy queues
   are capped at 1 MiB each; the stalled queue at 16 KiB. Chunks share immutable
   Python byte objects, not duplicated media payloads.
5. Verifies both files match the entire acquired container byte stream; compares
   demuxed packet metadata across consumers; decodes both outputs with errors fatal.
6. Compares source/output packet hashes, codec configuration, PTS/DTS/durations and
   packet flags, reporting differences rather than hiding them behind decode success.
7. Cleans temporary media and terminates/reaps workers on errors/timeouts.

The command's exit status tests fanout/decoding, **not lossless metadata round-trip**.
Read `packet_comparison` for that result. A first-container-byte measurement can be
just headers, not media or a decoded frame. Healthy consumers write files; decoding
is subsequently checked offline. This is not a late-join, real-time decoder,
RTSP reconnect, native worker, HomeKit, or resource-performance proof.

## Observed result

Environment: macOS arm64, FFmpeg 8.1.3. Three-second fixtures, with zero and two
B-frames. These are sample observations, not timing thresholds for automated tests.

| Check | No B-frames | Two B-frames |
| --- | --- | --- |
| Healthy consumers decode | Both | Both |
| Stalled subscriber detached | Yes | Yes |
| Encoded video/AAC payloads preserved | Yes | Yes |
| Video packets | 45 → 45 | 45 → 45 |
| Audio packets | 142 → 142 | 142 → 142 |
| Codec configuration hashes preserved | Yes | Yes |
| Missing video DTS | 0 → 0 | **0 → 2** |
| AAC duration/packet-flag metadata preserved | **No** | **No** |
| A/V PTS epoch shift agrees within output clock quantization | Yes | Yes |

No-B-frame output had a five-microsecond cross-track epoch-shift difference, below
one output clock tick. The tool reports both the actual spread and its tolerance.
The B-frame case shifted both tracks by about 133 ms. Container timestamp rebasing
is not inherently corruption, but consumers must share the same epoch.

### Decision

**Do not adopt raw NUT byte fanout as the production shared-media contract.** This
specific round trip preserves encoded payloads but not all packet timing metadata.
Container subscribers also cannot safely join at an arbitrary byte offset, recover
after overflow, or receive a codec-generation change without a demux/admission layer.

Prefer a supervised **libavformat packet worker with explicit framed IPC**. The
subsequent [local-file proof](packet-ingest-proof.md) preserves the tested packet
metadata exactly; RTSP and output-playback coverage remain required. Export codec
parameters, time bases, packet PTS/DTS/duration, keyframe flags and relevant side
data explicitly; distinguish missing timestamps from zero. Add bounded packet
lengths and generation markers. The native result covers its tested fixtures,
not every source format or a production-ready acquisition pipeline.

Keep FFmpeg output muxing/encoding initially. Do not implement RTSP or an MP4 muxer
from scratch merely to remove a subprocess. Do not use completed HSV fragments as
live-view input: that introduces fragment-duration latency.

## Remaining milestone-0 gates

1. **Native-worker local-file proof complete:** exact packet/timestamp/AAC-side-data
   preservation with two consumers, bounded slow-consumer isolation, cancellation
   and malformed-frame limits. See [results and scope](packet-ingest-proof.md).
   Output muxing/playback and broader codec-parameter coverage remain open.
2. **RTSP fixture:** authentication, interleaved RTP, missing/backward timestamps,
   silent socket stall, reconnect with changed SPS/PPS and late IDR admission.
3. **Both camera models:** IP8M-2696E-AI and IP8M-2796E-AI, actual RTSP acquisition
   and encoder-reset behavior. No `apply-settings --write` against an active bridge.
4. **Deployment baseline:** idle recording, cold live start, repeated warm start,
   recording + live audio/video, actual supported viewer concurrency, then clean
   shutdown. Record 30-second process/metric samples per steady-state scenario and
   repeated individual first-decoded-frame measurements on Apple clients.
5. **Fault baseline:** approved camera/network interruption during recording/live,
   recovery time, missing clips and real Home playback. Coordinate disruptive tests
   rather than changing a live security setup without agreement.

Only after these gates should shared acquisition replace the current readers.
Keep raw reports/media out of version control. A lower worker/connection count is
an architectural result; CPU, latency and reliability improvements require matched
before/after measurements on the same hardware and configuration.
