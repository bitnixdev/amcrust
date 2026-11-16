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
| Live video | FFmpeg packet copy/RTP followed by the shared, fenced Rust SRTP/SRTCP transport | Shared encoded H.264, Rust per-viewer packetization and existing encryption/control integration |
| Live audio | Per-viewer FFmpeg AAC decode/Opus encode | Rust-owned FFmpeg-library audio processing shared where negotiated settings match |
| Recording | Persistent FFmpeg reader/mux, Rust fMP4 prebuffer and HDS delivery | Shared packet subscriptions, Rust-owned mux/lifetimes, existing HomeKit/HDS integration |
| Experiments | Retained numeric findings; obsolete C/Python fixture harness removed | Behavioral evidence to carry forward, not a second production implementation |
| Rust acquisition boundary | `src/ingest.rs` owns libavformat; `src/source/` supplies an encoder registry, joined supervisors and bounded generation-scoped subscriptions | Connect the library owners to production live/audio/recording consumers |

**The production acquisition cutover has not happened.** Previous packet, lifetime,
mux and presentation experiments do not establish that the new Rust path is deployed
or accepted by Apple clients. The project-owned C workers and probe packaging have been removed;
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
  distinct from zero. The experimental AMC2 JSON/blob IPC and its fixture workers
  have been removed. Typed packets are the in-process contract; do not hex-serialize
  payloads per peer.
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
retracted. Source-generation transport fences, recording history/copies, aggregate output-job
limits and Prometheus wiring remain integration work. Logical charges are not a bound
on FFmpeg internal allocations, allocator overhead or process RSS.

## Implemented output-library boundary

`media::rtp::H264` packetizes a charged source delivery into bounded caller-provided
storage (single NAL or FU-A). Each viewer owns its sequence and RTP clock origin plus
negotiated SSRC/payload type/plaintext packet limit. SPS/PPS accompany each IDR; the
last packet alone carries the AU marker. Abandoning a partial AU fails that packetizer.
The shared secure transport below now owns production socket writes. Connecting this
packetizer and source-derived RTCP generation to HomeKit sessions remains integration
work; `Transport::rtp_capacity()` reserves the SRTP overhead from the wire limit.

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

## Implemented secure transport boundary

`media::transport::Transport` is now the **only production live SRTP/SRTCP socket-write
path**, used by both FFmpeg adapters in `src/stream.rs` and available directly to Rust
packetizers. It owns the UDP socket, fixed peer, outgoing SSRC and cryptographic state.
It does not acquire media, pace packets, rescale timestamps or invent sender reports.
The legacy timestamp adapter stays at the FFmpeg boundary only; Rust audio's 48-kHz
presentation timestamps must not pass through it.

Outgoing protected datagrams must fit the configured wire limit (maximum 2048 bytes), including
10 bytes for SRTP or 14 for SRTCP. Production reserves SRTP overhead in FFmpeg's packet
size, caps requested MTUs at 1378 bytes and refuses requests below 188 rather than
sending beyond them; invalid audio MTU disables audio without stopping video.
One send and one receive operation may be active per transport; concurrent calls are
refused, not queued. A write has a 250-ms readiness budget. WouldBlock retries reuse
ciphertext, not cryptographic counters. Feedback is bounded, restricted to the peer,
authenticated and replay-protected with a 128-packet window. Only the first authenticated
feedback SSRC is retained; a different sender requires a new session. Unauthenticated
SSRCs cannot accumulate retained replay state.

`revoke()` synchronously fences the nonblocking socket write under the same gate and
wakes pending waits. Session replacement/end/suspend/shutdown revoke every affected
track before the first awaited proxy cleanup, then abort/join proxy tasks. Drop revokes
and aborts but cannot join. Already kernel-accepted datagrams cannot be retracted.
The producer predicate is checked before encryption and immediately before writing,
but **is not an atomic source-generation fence**: integrating source retirement/privacy
still requires revoking its transports before acknowledging the transition. Aggregate
source/output accounting, recording-copy fences and joined source owners remain TODO.

Real-source H.264 from two Rust viewers decrypts/decodes through independent FFmpeg SRTP
receivers. This is not Home app acceptance. Production still uses FFmpeg camera readers
and their RTCP clock pairs/cadence; Rust now formats the outbound reports below.
Source-clock RTCP, audio receiver priming and shared-acquisition injection remain
required before those readers can be removed.

### Rust-owned outbound sender reports

`media::rtcp::Sender` formats compound SR/SDES from an **explicit NTP/RTP clock pair**
and accepted RTP counts. It never substitutes arrival time, zeroes tracks or infers
capture time. The NTP fractional bits and supplied RTP timestamp are preserved exactly.
One fresh random identity per prepared viewer session provides a common audio/video
CNAME without exposing the host name, camera address or HAP session ID. A new session
gets a new identity, and each track retains its independent SSRC and counters.

`Transport::rtp_counts()` counts successful nonblocking UDP writes under the send gate.
Octets exclude RTP headers, CSRCs, extensions, padding and SRTP overhead; malformed
lengths are refused before encryption. Counters wrap modulo 2^32 as required by RTCP.
They are kernel acceptance, **not receiver acknowledgment**. The session actor must
serialize its count snapshot/report construction with its sends; the production proxy
already does this in one task.

Production no longer forwards the FFmpeg sender-report payload verbatim. Its adapter
validates bounded compound framing and SR/BYE shapes, extracts the existing rescaled
clock pair and builds Rust SR/SDES with actual accepted counts. The legacy clock and
report cadence are unchanged. Other framed blocks, including FFmpeg's host-derived
SDES, are not forwarded. BYE accompanying an SR is appended to that compound; a bare
BYE retires the adapter without inventing a clock or sending unnegotiated reduced-size
RTCP. Controller feedback still goes to the legacy producer after authentication.

**Remaining integration:** qualify the source-to-NTP mapping, own report scheduling
and receiver-feedback handling in the Rust session actor, and connect source retirement.
A report formatter is not a clock policy or an audio preroll/trim contract. Neither
this API nor the accepted-write counters establishes Apple playback or common A/V
capture-clock accuracy.

## Implemented audio-processing boundary

`media::audio::Transcoder` owns AAC-LC decoding, fixed-ratio libswresample conversion,
a bounded PCM FIFO and libopus encoding on one blocking media worker. Configuration
covers rate, channels, bitrate, frame duration, VBR, FEC and expected loss; application
mode is fixed to voice. The admission pool limits jobs and retained output, but does
not yet share/deduplicate encoders with matching source/configuration. Native channel
layout, rate and sample format changes are refused rather than reused in an old resampler.

Decoder-frame PTS and sample counts are checked independently of private encoder sample
ordinals. No asynchronous resampling, silence injection, source-sample dropping or
wall-clock timestamp substitution is used. `Transcoder::new` retains strict timing:
`source_timestamp_48k` refuses observed clock contradictions and uncovered priming/tail
ranges. The explicit candidate below is a separately selected, guarded clock model,
not permission to promote arbitrary encoder ordinals to RTP timestamps. A locally
continuous sample clock alone still does not prove common A/V sender alignment.

Encoded Opus packets retain header/side data, cancellation/source grants and admission
charges. Native encoder padding counts use the configured sample rate; `decoder_skip`
converts them exactly to the Opus decoder's fixed 48-kHz domain. Output access has a
two-second residence limit; reservations stay charged until held packets drop. Explicit
finish flushes decoder, resampler and encoder tails; source EOF/cancellation is not
finish. Codec decoding/count checks are not continuous-source PCM equality, listening
acceptance or a production audio path. The observed real-camera clock contradictions
remain a blocker for unqualified transport integration, not a reason to adopt
unanchored packet-order clocks.

### Candidate live-audio decision and revisit guide

**Selected implementation: source-anchored, bounded sample progression.** Explicitly
call `Transcoder::new_with_clock_policy(..., ClockPolicy::candidate(), ...)`; the
ordinary constructor remains strict. The candidate requires **48-kHz AAC input** and
supports **8/16/24/48-kHz Opus encoder input with 20/40/60-ms frames**. This includes
HomeKit's negotiated rates and packet times handled by the existing live path.
Other configurations are refused, not rounded. Encoder-private positions and padding
are converted exactly into the Opus decoder/RTP clock's fixed 48-kHz domain.

1. Wait on the existing audio subscription for an AAC packet carrying public SR and
   RTCP-derived PRFT metadata. Constructor refusal is `Incomplete` until then. Start
   the decoder at that admitted packet and anchor to its first decoded source PTS,
   preserving the source's signed common-epoch offset rather than zeroing audio.
   Late admission can wait roughly an SR period (~5 seconds). If that startup delay
   is unacceptable, qualify a generation-owned anchor before admitting viewers;
   do not bypass evidence requirements or independently zero the new viewer.
2. Advance presentation by actual decoded PCM sample count at 48 kHz. Later sender
   reports refresh evidence but **never move that anchor**. Keep original packet
   clocks and all mismatch statistics; no source packet is rewritten.
3. Compare every decoded-frame PTS with the anchored sample prediction. Refuse the
   processor if absolute phase exceeds **4800 samples / 100 ms**, or no distinct
   SR-bearing packet has been read within **15 seconds**. `clock_status()` exposes
   current/maximum phase and last-report age to distinguish drift from stale evidence.
   PRFT provenance must remain supported on every input packet. These are provisional
   operational guardrails,
   not perceptual tolerances or a claim that NTP is physically correct. The freshness
   interval is approximately three observed camera SR periods, not a fixture deadline
   or raw network RTCP receive time. Stricter limits may be selected; larger limits
   require revisiting the implementation and decision.
4. A failure is sticky and revokes this processor's held output. Other source consumers
   survive. Do not automatically reanchor or repeatedly restart it into the same live
   session to conceal drift. A new processor has a fresh anchor; source generations
   cannot share decoder/clock state. Future pooling must include the full clock policy
   as well as source generation and codec settings, and must not splice independently
   started processors with different anchors/decoder histories.
5. `presentation_48k` describes the complete encoded packet, its audible source-time
   interval and effective decoder trims. Initial Opus pre-skip can be in `OpusHead`
   without packet side data; apply it **once**, not again to already-trimmed PCM.
   End discard is retained. Only samples covered by actual decoded input and converted
   output are mapped. After successful local finish, a resampler endpoint extending
   past the source by **less than one encoder-rate sample** is explicitly discarded
   in 48-kHz units; `resampler_tail_discard_48k` exposes this additional trim. Native
   packet timestamps and side data remain unchanged; consumers need the complete
   `presentation_48k().decoder_skip`, not just the native-padding accessor. A shorter
   final Opus packet is allowed when its native duration/padding describes a legal
   2.5-ms-multiple span within the configured packet time.
   **Undercoverage, a whole-sample-or-larger excess, and live/pending endpoint cropping
   are refused**, never padded or relabeled as complete. In particular a 199-AAC-AU
   window at 16 kHz returned one 48-kHz tick less output duration; `finish()` fails
   with `Incomplete` and revokes held outputs. That is a finite-endpoint refusal, not
   a refusal of the live mode. No successful clip/end acknowledgment follows from it.
   Unsupported shapes are refused. `source_timestamp_48k` still
   refuses packets requiring trimming. A live RTP receiver does not automatically
   receive OpusHead/skip side data: transport integration must explicitly establish
   preroll/trim behavior or refuse the boundary, never drop a needed first packet.

This is a **live-audio qualification candidate**, not an HSV edit/cut policy. It does
not settle AAC decoder-history/PNS differences, capture-clock accuracy, controller
startup, or audible lip-sync acceptance. Decoder callbacks may buffer SILK resampling
output across packets; callback sizes alone are not coded-packet durations. Receiver
integration must honor the full presentation contract and its own decoder latency. No HomeKit
path is switched by this library API. The sole execution backlog remains in TODO.

| Alternative retained for reconsideration | Why not selected now | Symptoms favoring reconsideration / evidence our choice is wrong |
|---|---|---|
| Strict source-PTS progression (still the default) | Real AAC has frequent non-contiguous steps, so usable timestamps are refused | Corrected firmware or another camera provides contiguous sample timing; candidate offers no benefit and should not normalize unnecessarily |
| Follow each SR/PRFT-derived timestamp directly | PRFT and demux PTS share SR provenance; observed report-boundary jumps remain | Independent capture/client evidence shows the reports accurately represent real gaps, while our continuous clock compresses gaps or develops offset after stalls |
| Retain/recover per-packet raw RTP clocks before SR-based demux adjustments | Public SR RTP values describe report instants, not each media packet; PRFT is dependent evidence. This needs a justified public-API packet mapping, not private FFmpeg access | If independent packet evidence shows raw RTP follows decoded samples while all large jumps come from SR remapping, prefer preserving that clock over modeling it; unexplained rollover/phase jumps would indicate a bad reconstruction |
| Continuous sample clock with a fixed source anchor (**chosen, bounded**) | Avoids repeated phase jumps without changing PCM, but assumes the nominal audio sample rate remains adequate | Monotonically growing phase, repeated 100-ms trips, lip sync progressively worsening in long views, or fresh sessions repeatedly starting at different A/V offsets; do not merely raise the limit |
| Slow bounded clock recovery plus asynchronous resampling | Adds a PLL/filter, rate limits and real sample modification before oscillator drift is distinguished from report jitter | Sustained, repeatable phase slope and corroborating A/V drift favor this; audible pitch modulation, oscillating lip sync or repeated corrections after each SR would indicate chasing jitter instead |
| Host-arrival clock or unanchored packet/frame counting | Network buffering is not capture time; independent track zeroing loses A/V offset | Latency changes with network load, burst arrivals producing speed changes, or session-dependent lip sync; use arrival time only for deadlines |
| Insert/drop samples or encoded packets at discontinuities | Risks clicks, lost speech and decoder-state damage; hides whether a jump was real | Audible clicks, clipped first syllables, gaps or repeated syllables at SR boundaries, and sample-count discrepancies mean the repair is damaging content |

Priming-specific wrong-choice symptoms: clipped onsets suggest double pre-skip or
insufficient AAC history; an initial noise/silence lead-in suggests missing Opus
pre-skip; click/noise or excess audio at the end suggests discarded tail metadata;
a fixed offset from the first frame suggests an incorrect anchor/delay convention.
Stable sample counts do not rule out any of these perceptual failures.

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
