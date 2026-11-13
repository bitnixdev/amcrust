# Packet-aware ingest worker proof

**Scope:** milestone-0 local-file experiment, not production acquisition. The
worker is a separate opt-in Nix package; it is not linked into amcrust, added to
its runtime closure, or enabled by the NixOS service.

## Reproduce

```sh
nix build .#ingest-worker-probe --no-link -L
python3 -B -m unittest discover -s scripts -p 'test_*.py' -v
```

The Nix package compiles `probes/ingest_worker.c` with warnings treated as errors,
then runs the Python unit suite and `scripts/packet_ingest_probe.py` against that
binary. FFmpeg, libavformat headers/libraries, ffprobe and Python come from the
repository's locked Nix inputs. Nothing is installed system-wide. Build results
may be cached; the package's check phase is part of its build.

The probe generates/deletes its own H.264/AAC files. It does not read `.env`, access
cameras, change camera settings, or retain media. The default worker invocation
allows only FFmpeg's `file` protocol; a later opt-in mode supports the generated
[loopback RTSP fixture](rtsp-ingest-proof.md). It can block on input/output; the
supervisor owns timeouts and cleanup. Do not use it as a production network service.

## Result

On macOS arm64 with the repository's FFmpeg **9.0.2**, both three-second fixtures
(640×360/15 fps, mono 48 kHz AAC, zero/two H.264 B-frames) passed:

- 187 packets each: 45 video + 142 audio.
- Exact integer PTS, DTS, duration, stream identity, flags and encoded-payload hashes
  match a separate ffprobe demux of the original file, in original packet order.
- Codec name, time base and extradata hashes match; dimensions, level, audio sample
  rate and channel count match where ffprobe reports them.
- Two AAC skip/discard side-data packets per fixture match byte-for-byte, including
  skip counts, discard padding and reason bytes.
- Two independently queued consumers receive the same packet summaries.
- A third subscriber stops consuming media after setup; it is detached on overflow
  without blocking or losing packets for the healthy consumers.
- A native worker blocked opening a FIFO is killed and reaped when its supervisor
  is cancelled. Missing input is reported as failure, never successful empty input.

This addresses the metadata loss seen in the earlier NUT round trip. That earlier
experiment used FFmpeg 8.1.3; this is an independent preservation check against the
**same library version** as this worker, not a cross-version performance comparison.
The harness rejects worker/ffprobe libavcodec version mismatches.

No remuxed output or Apple playback is claimed here: consumers validate packets,
not rendered frames. Missing/null timestamp behavior is covered by protocol tests;
these generated MP4 fixtures have valid timestamps. Stream-side-data cases beyond
these fixtures deliberately fail the comparison rather than silently claim coverage.

## Experimental wire contract (version 1)

A frame is a four-byte big-endian unsigned JSON-body length, then that many UTF-8
bytes. JSON integers retain signed 64-bit timestamp precision; timestamps are
never floating-point seconds. Bytes use lowercase hex for inspectability. This is
not an efficient production serialization or a stable public protocol.

Order:

1. `hello`: version, generation (1 for local files; supervisor-assigned for the
   loopback RTSP mode), stream count, libavcodec version and the build's
   `AV_PKT_DATA_SKIP_SAMPLES` enum value.
2. One `stream` per index: codec/media names, time-base numerator/denominator,
   profile/level, dimensions, sample rate/channel count, initial/trailing padding,
   seek preroll, codec extradata and coded side data. The output proof additionally
   exports channel layout, frame size, format, sample aspect ratio and color metadata.
3. `packet`: generation, contiguous global sequence, stream index, PTS, DTS,
   duration, raw AVPacket flags, encoded data, and every packet-side-data type/data.
4. `end`: generation and exact final packet count. EOF without this is failure.

`AV_NOPTS_VALUE` is **null**, distinct from timestamp zero. Negative timestamps and
composition offsets are preserved. Side-data type IDs are FFmpeg-version-specific;
there is no claim of cross-version ABI stability. Descriptors are a useful subset
of AVCodecParameters, not a complete serialization of every possible codec/layout.
The Python consumer deliberately accepts only the H.264/AAC fixture codecs.

Both boundaries enforce limits:

| Resource | Limit |
| --- | --- |
| JSON frame body | 16 MiB |
| Packet payload / codec extradata | 4 MiB |
| Aggregate side data per packet/descriptor | 1 MiB, at most 64 entries |
| Streams | 8 |
| Healthy subscriber queue | 32 MiB of raw frames and 256 frames |
| Stalled media subscriber | 16 MiB and 1 frame |

Queue budgets exclude interpreter object overhead, the in-flight frame and pipe
buffers; they are not a total RSS promise. The C worker has a reusable 16 MiB frame
buffer; hex doubles payload size. Subscribers share immutable raw frame objects.
On overflow, clear and detach the subscriber rather than drop arbitrary encoded
samples and pretend continuity. A production live consumer will additionally need
media-duration budgets and IDR admission before it can recover.

The receiver validates framing before allocating a body, requires configuration
before packets, rejects unknown versions/generations, checks exact sequence/count,
and rejects trailing frames. Tests cover fragmented/coalesced reads, oversized and
truncated bodies, malformed schemas, invalid byte fields, signed/missing/large
integer timestamps and independent byte/count queue bounds.

## Subscriber terminal signals

The original `PacketQueue.detach()` cleared data but left an already waiting
`receive()` blocked. The experimental queue now closes permanently with a stable
reason and wakes **all** waiting receivers. `receive()` raises `QueueClosed` with
one of `overflow`, `generation_changed`, `source_failed`, `source_complete` or
`cancelled`. Repeated closure preserves the first cause; later offers are refused.

This is abort/discard behavior, not a graceful drain operation. Buffered packets
are cleared immediately and cannot leak through a receiver awakened just before
closure. Cancelling one waiting receiver does not reserve/lose a packet or consume
another receiver's readiness notification. The queue remains single-event-loop,
byte/count bounded and nonblocking for the producer; a stalled peer does not close
a healthy subscriber.

Finite-file consumers now read until the explicit IPC `end` frame rather than
polling a separate completion flag. Source failure/cancellation closes their queues
before worker cleanup. `source_complete` is used only after finite input and consumer
end counts have succeeded and buffers have drained; it is **not** recording delivery
or iCloud acceptance. Live RTSP EOF remains failure. Generation reset and source
failure close old live-admission queues with distinct terminal reasons before
replacing state or waiting for worker cleanup.

Regression cases cover multiple blocked receivers, offer/close races, cancellation,
first-cause preservation, peer isolation, and generation/source-failure wakeups.
No timer polling or sentinel media frame is needed to deliver closure.

## Follow-up gates

The preservation result supports proceeding with the packet-aware worker direction.
It does **not** make this worker production-ready.

1. **Synthetic RTSP fixture complete:** authentication, TCP-interleaved H.264/AAC,
   silence/stall, disconnect and reconnect with changed SPS/PPS. See the
   [RTSP proof and limitations](rtsp-ingest-proof.md).
2. **Generation/admission prototype complete:** supervisor-assigned generations,
   invalidation on failure, and late-viewer IDR admission with decoded-video checks.
   Experimental subscriber terminal signals now exist; production retry/backoff
   and terminal-event integration remain open.
3. Concurrent recording/live load and time-based buffer budgets remain open.
   Slow-subscriber isolation currently has synthetic fanout/queue coverage.
4. **Finite-file output proof complete with a limitation:** [A/V reconstruction](av-output-proof.md)
   preserves packet timing/payloads, video and the PCM prefix, but one fixture retains
   terminal AAC padding. The [clocked RTSP proof](rtsp-clock-proof.md) now repairs
   bounded non-reordering startup timestamps with shared-epoch and codec evidence. Unclocked/ambiguous
   input is still rejected; real-camera/HomeKit acceptance remains open.
5. Replace hex JSON with a bounded, versioned binary packet representation before
   treating this as a performance design. Export additional codec/layout/color
   metadata as required by supported consumers.

Production still uses its original FFmpeg readers. The local living plan tracks
these open gates, including the two real-camera models and deployment baseline.
