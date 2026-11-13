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

## Experimental wire contract (AMC2)

Both native workers now use a **versioned binary envelope**. The old hex-JSON wire
format has no fallback. The body is:

```
u32 body_length                 # outer transport prefix, excluded from body
"AMC2"                          # envelope version/magic, four bytes
u32 metadata_length
u32 blob_length
metadata_length bytes           # UTF-8 JSON, logical metadata schema version 1
blob_length bytes               # raw encoded data / extradata / side data
```

All length words are unsigned big-endian. Metadata byte fields are objects of
exactly `{offset, length}` into the blob. The primary payload/extradata comes first,
then side data in array order. References must be contiguous and consume the entire
blob: no gaps, overlaps, extra bytes or out-of-range references. Empty byte fields
are representable; consumers may still reject them where codec data is required.
The total body length must exactly match its two bounded sections.

JSON integers retain signed 64-bit timestamp precision; timestamps are never
floating-point seconds. The payload is no longer encoded as hex on the wire or
inside subscriber queue bodies. Python proof objects still expose hex for existing
bitstream/ffprobe comparisons, through explicit encode/decode adapters. This is not
a stable public ABI, a zero-copy design, or a throughput claim.

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
| Metadata | 64 KiB, bounded nesting |
| Binary blob | 5 MiB |
| Envelope body | 12 bytes + metadata + blob; at most 5 MiB + 64 KiB + 12 bytes |
| Packet payload / codec extradata | 4 MiB |
| Aggregate side data per packet/descriptor | 1 MiB, at most 64 entries |
| Streams | 8 |
| Healthy subscriber queue | 32 MiB of raw frames and 256 frames |
| Stalled media subscriber | 16 MiB and 1 frame |

Queue budgets exclude interpreter object overhead, the in-flight frame and pipe
buffers; they are not a total RSS promise. The C worker has a reusable 64 KiB
metadata buffer and writes bounded raw blob slices before releasing the packet.
Subscribers share immutable binary frame bodies. The sink copies byte fields into
FFmpeg-owned buffers with decoder padding, not native-struct serialization.
On overflow, clear and detach the subscriber rather than drop arbitrary encoded
samples and pretend continuity. The experimental media-duration bound is described
below; production recovery still needs duration policy and fresh IDR admission.
Clock startup also budgets the expanded serialized proof objects it retains, so
smaller wire frames do not silently weaken that existing retention bound.

The receiver validates the outer length before allocating a body, requires configuration
before packets, rejects unknown versions/generations, checks exact sequence/count,
and rejects trailing frames. Tests cover fragmented/coalesced reads, oversized and
truncated bodies, malformed schemas, invalid byte fields, signed/missing/large
integer timestamps and independent byte/count queue bounds. Twenty shared malformed
binary vectors are rejected by both the Python decoder and native mux sink, including
unknown magic/legacy JSON, section lengths, gaps/overlaps, unused bytes, invalid
reference integers, byte/count limits and excessive metadata nesting. Native cases
corrupt a packet after output creation and also check partial-output removal.

Observed wire sizes for the same 187-packet fixtures, including outer prefixes:

| Fixture | AMC2 bytes | Equivalent compact hex-JSON bytes |
| --- | ---: | ---: |
| No B-frames | 503,014 | 965,900 |
| Two B-frames | 488,059 | 935,973 |

This is about **48% fewer serialized bytes**, measured against equivalent metadata
and payloads, not a CPU/RSS/latency benchmark or a comparison across FFmpeg versions.

## Binary consumer views

Packet fanout consumers and `MediaQueue` validation now use `Protocol(binary=True)`.
Byte fields are independent, read-only `memoryview` slices into the immutable AMC2
body, not payload-sized hex strings or decoded byte copies. Hashing and side-data
comparison consume those views directly. Native no-B/B-frame fanout asserts that
payload, extradata and side-data views actually refer to the received wire body,
as well as preserving the existing exact packet comparisons.

Only backing bytes are shared: each decode gets its own metadata and view objects.
Releasing one consumer's view or changing its metadata cannot invalidate another
consumer. Retained codec data survives later reads; a delivered view remains valid
after peer detach, queue draining and source-reference release. Byte-field validation
rejects read-only aliases of mutable storage, noncontiguous views and non-byte casts.

Delivered views retain their whole backing frame until released. They are outside
queue byte/duration accounting; this is not an in-flight memory limit. Closing a
queue clears its own descriptors/views, but cannot revoke data already handed out.

The explicit inspection mode still returns hex for the source-side ffprobe oracle,
clock repair and H.264 evidence tooling. Metadata parsing/lifecycle validation still
occur separately per consumer. Sharing validated immutable metadata and sustained
load/memory measurement remain next steps; no end-to-end zero-copy or CPU/RSS
improvement is claimed here.

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

## Media-duration budgets after clock admission

`MediaQueue` adds a third independent limit to the byte/count queue. It accepts
only immutable, generation-validated wire frames with known PTS/DTS, positive
packet durations and increasing per-track DTS. Reordered **PTS** is allowed;
timestamps and packet contents are never rewritten. Invalid clocks/schema close
the queue with `source_failed`; exceeding any queue budget closes only that
subscriber with `overflow`, clears retained media and wakes its receivers.

The caller must first establish a common A/V timeline (file demuxing or the
[RTSP clock admission stage](rtsp-clock-proof.md)). This queue does not establish
clock synchronization and is not attached to the earlier raw-RTSP admission probe.

For each packet, in exact rational seconds:

- Start = `min(PTS, DTS) * time_base`.
- End = `(max(PTS, DTS) + duration) * time_base`.
- Progress = greatest end admitted in this generation, including consumed packets.
- Retained media span = progress minus the earliest start still queued; zero when
  no media packets remain. Reject an offer if its prospective span exceeds budget.

This counts timestamp gaps and cross-track offsets rather than summing durations
or independently zeroing tracks. Retaining generation-wide progress also prevents
a late track from escaping the limit after the leading track's data was consumed.
Decode/presentation lead counts toward the budget, so a configured limit must be
large enough for the codec's reorder depth as well as the desired subscriber lag.
Exact boundary equality is accepted; negative origins and unequal time bases work.

The file fanout now has two healthy duration-bounded consumers and separate stalled
count-limited and duration-limited consumers. Both no-B and B-frame native fixtures
retain exact packet equality while the duration-stalled peer detaches at a **0.5 s
synthetic budget**, before its byte/count caps. Clock-admitted RTSP fixtures also
pass an unchanged-byte queue check, including delayed audio and Main/High profiles,
while a stalled peer exceeds the same media budget.

Unit coverage includes gaps, cross-track progress, reordered PTS/decode lead,
fractional boundaries, dequeue accounting, cancellation, terminal wakeups, generation
mismatch, unknown/invalid clocks, and byte/count backstops. Repeated draining keeps
per-packet accounting empty rather than accumulating it.

**Limits:** this is not a wall-clock residence deadline, decoder/in-flight memory
limit, throughput measurement, or production latency recommendation. Source idle
expiry remains a separate supervisor/clock responsibility. Retained descriptor
state has the protocol's own stream/configuration caps and is not included in queued
wire-byte counts. Accounting scans at most the bounded queued frame count. Queue
validation and packet fanout consumers now use binary views instead of hex, but
metadata parsing per subscriber and source-side inspection objects remain proof
scaffolding. Shared validated metadata and sustained load/memory measurements remain
open before performance claims.

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
3. Media-duration budgets now have synthetic file and clock-admitted RTSP coverage.
   Concurrent recording/live load, in-flight budgets and residence deadlines remain
   open; this does not establish sustained performance or end-to-end latency.
4. **Finite-file output proof complete with a limitation:** [A/V reconstruction](av-output-proof.md)
   preserves packet timing/payloads, video and the PCM prefix, but one fixture retains
   terminal AAC padding. The [clocked RTSP proof](rtsp-clock-proof.md) now repairs
   bounded non-reordering startup timestamps with shared-epoch and codec evidence. Unclocked/ambiguous
   input is still rejected; real-camera/HomeKit acceptance remains open.
5. **Bounded binary framing and consumer payload views complete for the prototype.**
   Share validated metadata and remove remaining source-side inspection conversions
   before treating this as a performance design. Export additional codec/layout/color
   metadata as required by supported consumers.

Production still uses its original FFmpeg readers. The local living plan tracks
these open gates, including the two real-camera models and deployment baseline.
