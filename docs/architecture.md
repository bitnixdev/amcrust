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
| Rust acquisition boundary | `src/ingest.rs` owns blocking libavformat reads and immutable packet/configuration lifetimes; unused `native/` draft removed | Connect a joined source supervisor, registry and bounded subscriptions to production consumers |

**The production acquisition cutover has not happened.** Previous packet, lifetime,
mux and presentation experiments do not establish that the new Rust path is deployed
or accepted by Apple clients. The unused C-based draft and packaging have been removed;
the new Rust library reader is built but not connected to production consumers.

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
  muxing/audio integration remains to be implemented.
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
