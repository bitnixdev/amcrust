# Architecture and direction

**Decision: Rust application code, one shared acquisition per active camera encoder,
independent consumer transports.** Use upstream FFmpeg libraries through Rust bindings
for media work; do not maintain our own C workers or put a relay server in the middle.

This is the authoritative architecture. Execution order is in [TODO](todo.md);
completed work and its limitations are in [Evidence](evidence.md). The
[HDS wire reference](hds-wire-format.md) describes the existing transport protocol.

## Current state versus destination

| Area | Current state | Destination |
|---|---|---|
| Camera acquisition | Live video, live audio and recording open their own FFmpeg RTSP readers | One Rust-owned libavformat acquisition per active encoder |
| Live video | FFmpeg packet copy/RTP followed by Rust SRTP/SRTCP handling | Shared encoded H.264, Rust per-viewer packetization and existing encryption/control integration |
| Live audio | Per-viewer FFmpeg AAC decode/Opus encode | Rust-owned FFmpeg-library audio processing shared where negotiated settings match |
| Recording | Persistent FFmpeg reader/mux, Rust fMP4 prebuffer and HDS delivery | Shared packet subscriptions, Rust-owned mux/lifetimes, existing HomeKit/HDS integration |
| Experiments | Python prototypes plus C FFmpeg wrappers | Behavioral evidence to carry forward, not a second production implementation |
| Rust acquisition boundary | `src/ingest.rs` owns libavformat; `src/source/` supplies an encoder registry, joined supervisors and bounded generation-scoped subscriptions | Connect the library owners to production live/audio/recording consumers |

**The production acquisition cutover has not happened.** Previous packet, lifetime,
mux and presentation experiments do not establish that the new Rust path is deployed
or accepted by Apple clients. The unused C-based draft and packaging have been removed;
the Rust library registry/fanout is built and exercised against the real camera, but
not connected to production consumers.

## Intended data flow

```text
One amcrust instance per camera
  Camera configuration / encoder capabilities / privacy state
    └─ Rust source registry (camera + encoder identity)
         ├─ Main encoder: one libavformat RTSP acquisition when needed
         └─ Sub Stream 2: one acquisition when needed for the selected live mode
              ↓ generation-bound encoded packets + codec state + clock evidence
         Rust admission, bounded fanout and consumer leases
              ├─ Live video → independent RTP/RTCP → SRTP/SRTCP per viewer
              ├─ Live audio → compatible shared AAC→Opus processing
              │                → independent RTP/RTCP/SRTP per viewer
              └─ Recording → bounded prebuffer/history → clip-local cut
                              → libavformat fMP4 → owned HDS output job
```

The main encoder is shared by recording, applicable live video and live audio;
requesting both tracks must not open two camera sessions. Distinct hardware encoders
can legitimately need distinct acquisitions. The registry must not key a source by
viewer, recording session or requested output format.

## Rust and FFmpeg boundary

- Required demux, packet/codec ownership, mux and audio behavior currently wrapped in
  project C must be exposed through **Rust bindings to public FFmpeg APIs**. Upstream
  FFmpeg is still a native library dependency; this is not a codec rewrite in Rust.
- Select bindings compatible with the repository-pinned FFmpeg API. Missing safe
  wrappers may require a small audited **Rust FFI module**, not a project C shim or
  access to private FFmpeg structs. `ffmpeg-sys-next` is pinned to 9.1.1 against
  Nix FFmpeg 9.0.2. `src/ingest.rs` contains the acquisition FFI ownership boundary;
  `src/media/fmp4.rs` owns the finite mux boundary and `src/media/audio.rs` owns
  codec/resampler state. Production mux/audio integration remains to be implemented.
- Blocking I/O belongs off the async executor, with interruptible reads, deadlines and
  explicit shutdown/join. If process isolation proves necessary, use a Rust worker
  with owned lifetime; do not reinstate the standalone C-worker architecture.
- Prefer reference-counted encoded packets and typed immutable metadata in Rust.
  Keep exact rational time bases and signed integer clocks; unknown timestamps are
  distinct from zero. Experimental AMC2 JSON/blob IPC is not a required production boundary. Keep any
  diagnostic parser separate from the data path; do not hex-serialize payloads per peer.
- Do not reimplement RTSP or MP4 from scratch. Keep video packet-copy. Rust should own
  session RTP packetization, lifecycle, cancellation, accounting and privacy fences.
- Dependencies and native-library versions belong in Cargo/Nix, not global installs.

## Implemented source-library boundary

`source::Registry` registers camera/encoder identities once, retaining credentials in
its supervisors. Consumer handles cannot retrieve URLs. An idle source has no reader;
the first subscriber starts one blocking libavformat job, shared across requested
tracks. Failure retires all old leases. New subscriptions observe bounded backoff and
a consecutive-failure circuit breaker; they never resume inside an old generation.
Explicit `Registry::shutdown` signals all owners before joining and can be awaited
again if cancelled. Call it before dropping the Tokio runtime; Drop signals cleanup
but cannot provide an async join by itself.

Current admission supports Annex-B H.264 with one matching SPS/PPS pair and checked
slice references. It waits for a future actual timestamped IDR, never a cached GOP or
key flag alone. Initial unclocked packets are skipped, not repaired; later clock loss
or non-increasing DTS is refused. Irregular steps are preserved. These decisions do
not establish source A/V alignment or decoded presentation.
AudioVideo subscriptions start at video admission, not at a covering historical AAC
window; they are not a finished recording-cut implementation.

`Source::clock_snapshot` exposes bounded, numeric per-generation observations of
public RTCP SR and producer-reference (PRFT) side data, timestamp/duration differences,
report ages and relative epoch spread. It retains no media or URLs. SR metadata is
attached to a following media packet, not necessarily one sampled at the SR instant.
In pinned FFmpeg 9.0.2, PRFT is derived from the packet RTP timestamp and latest SR;
it is not an independent capture clock or the host's arrival time. The demuxer also
uses the latest SR to map multi-track PTS. These observations never rewrite PTS/DTS,
repair missing clocks, change resampling, or grant transport timing. Display rounding
is confined to diagnostics; original integer/rational metadata remains untouched.

Malformed SR/PRFT shapes are refused at acquisition. A report SSRC change,
non-advancing/ambiguous report clock, or unexpected PRFT provenance retires the source
before that packet is published. Modular wrapping is handled within the half-range
constraint; duplicates do not refresh report age. Retirement is explicit and a new
reader gets fresh observations. No report-age deadline, drift tolerance or affine
clock correction is inferred from the short device samples. The observed AAC clock
contradictions, including larger SR-boundary jumps, remain unresolved for live A/V.

Queue and held-delivery charges persist through retirement until owners drop. Source
and registry budgets include bounded reader handoff and descriptor charges. Access
checks and lease retirement are cooperative: already borrowed/copied bytes cannot be
retracted. Transport write fences, recording history/copies, aggregate output-job
limits and Prometheus wiring remain integration work. Logical charges are not a bound
on FFmpeg internal allocations, allocator overhead or process RSS.

## Implemented output-library boundary

`media::rtp::H264` packetizes a charged source delivery into bounded caller-provided
storage (single NAL or FU-A). Each viewer owns its sequence and RTP clock origin plus
negotiated SSRC/payload type/plaintext packet limit. SPS/PPS accompany each IDR; the
last packet alone carries the AU marker. Abandoning a partial AU fails that packetizer.
Transport writes, RTCP and SRTP remain production-integration work; SRTP overhead must
be reserved separately from the negotiated packet limit.

`media::fmp4::Muxer` uses public FFmpeg parameter-copy, packet-reference and custom AVIO
APIs for finite H.264/AAC or video-only output. Its first input must be the source-issued
IDR used to create it. A single exact video-PTS origin shifts every track; unrepresentable
shifts/rescales are refused. No guessed clocks or independent track zeroing are added.
FFmpeg derives MP4 sample durations from DTS where the camera's declared durations
conflict; the real check records this difference, not a presentation acceptance claim.
Covering AAC history, decoder preroll, actual clip cuts and tail policy remain unresolved.

Mux state is thread-bound and should run on a blocking media worker. A shared pool
reserves finite input/output and chunk allowances before admission. Copied chunks keep
the job reservation and source grant, rechecking cancellation, lifetime and generation
before access. Abandonment is not finish; explicit finish is only local mux completion.
These checks cannot retract already borrowed/copied bytes and do not yet fence HDS
writes. Reservations are logical ownership bounds, not FFmpeg allocator/RSS measurements.

## Implemented audio-processing boundary

`media::audio::Transcoder` owns AAC-LC decoding, fixed-ratio libswresample conversion,
a bounded PCM FIFO and libopus encoding on one blocking media worker. Configuration
covers rate, channels, bitrate, frame duration, VBR, FEC and expected loss; application
mode is fixed to voice. The admission pool limits jobs and retained output, but does
not yet share/deduplicate encoders with matching source/configuration. Native channel
layout, rate and sample format changes are refused rather than reused in an old resampler.

Decoder-frame PTS and sample counts are checked independently of private encoder sample
ordinals. No asynchronous resampling, silence injection, source-sample dropping or
wall-clock timestamp substitution is used. `source_timestamp_48k` refuses observed
clock contradictions and uncovered priming/tail ranges. A locally contiguous sample
clock alone still does not prove common video/audio sender-report alignment. Codec
ordinals exposed for mux/decoder bookkeeping must not be promoted to transport clocks.

Encoded Opus packets retain header/side data, cancellation/source grants and admission
charges. Native encoder padding counts use the configured sample rate; `decoder_skip`
converts them exactly to the Opus decoder's fixed 48-kHz domain. Output access has a
two-second residence limit; reservations stay charged until held packets drop. Explicit
finish flushes decoder, resampler and encoder tails; source EOF/cancellation is not
finish. Codec decoding/count checks are not continuous-source PCM equality, listening
acceptance or a production audio path. The observed real-camera clock contradictions
remain a blocker for transport integration, not a reason to adopt packet-order clocks.

## Required invariants

1. **One owner per source generation.** Reconnect, codec change or clock discontinuity
   invalidates affected consumers and configuration before reopening. Reap/join old
   work; use bounded retry/backoff. A failed source must not contaminate another camera.
2. **No fabricated source clock.** Preserve valid PTS/DTS, durations, codec/side data
   and the common A/V epoch. Missing timestamps, reordering and real-camera jitter need
   an explicit policy backed by device observations. Profile names, `video_delay=0`,
   packet order or equal-looking counters alone do not prove a decode clock.
3. **Late video admission needs a current IDR and current SPS/PPS.** Do not replay a
   stale GOP, grant admission from a key flag alone, or reuse retired codec state.
4. **Slow consumers fail locally.** Bound queued bytes/count/media span, outstanding
   deliveries, residence time, cached/pinned history and copied output separately.
   Bound aggregate jobs and bytes too. Overflow cannot silently drop encoded packets
   while claiming decoder continuity, or block healthy readers behind a stalled peer.
5. **Sharing ends before session transport.** Each viewer owns its SSRC, sequence,
   negotiated payload/MTU, RTP clock mapping, RTCP feedback and SRTP/SRTCP state. Audio
   processing is pooled only for compatible codec/rate/channel/bitrate/frame/FEC settings.
6. **Recording completion is local, not source EOF.** Cuts preserve the admitted
   selection and common epoch. Queued later packets are explicitly outside a cut;
   private mux counters never rewrite shared-source counters. Camera failure is not
   a completed clip or an acknowledged cloud recording.
7. **Privacy covers copied output.** Retire grants and stop later transport writes
   before awaited cleanup. Mux jobs, pending writes and completion operations have
   explicit owners and deadlines. Already accepted bytes/escaped copies cannot be
   retracted; zero managed owners does not prove erasure, zero allocations or low RSS.
8. **No fragment-sized live latency.** Live video consumes packets, not completed HSV
   fragments. Recording prebuffering and live-start admission have different purposes.

## Decisions deliberately not hidden by the architecture

The real camera sample already shows missing initial video timestamps and nonuniform
packet clock steps. The strict synthetic clock policy cannot simply be renamed
production code. AAC packet coverage is not continuous-decoder PCM equality; front
edit behavior is not end-trim behavior. Apple clients must decide whether the actual
presentation works. See [Evidence](evidence.md#media-boundaries-and-presentation).

## Cutover and acceptance policy

Replace the direct-reader callers as the shared source is connected; do not ship a
runtime switch or silent fallback to the old camera-reader pipeline. Internal work
can be incremental, but the completion claim requires every live/audio/recording
reader to use the shared source, including the real-device settings check.

**No more synthetic-testing work for this cutover.** Existing experiments are reference
material, not the next milestone. Build/type-check the Rust work, run the replacement
against real devices, and ask the user for Home app live/audio/HSV evaluation. Camera
encoder writes are authorized for that testing; coordinate competing instances so
pairing names do not conceal conflicting control of the same physical encoder.

Use Apple OS 27 clients; no legacy-OS compatibility branch is required. MediaMTX,
NUT/container-byte fanout, maintaining project C, and preserving a selectable legacy
acquisition path are not alternatives on the current roadmap.
