# Established findings and current limits

This is the consolidated record of work already done, **not a second backlog**.
[Architecture](architecture.md) states the chosen design; [TODO](todo.md) is the only
implementation sequence. Old per-milestone proof/research Markdown has been replaced
by this summary. Numeric artifacts remain linked below; implementation details remain
in the scripts/code and version control, not in a second archive of planning documents.

## What is actually running

The existing Rust bridge still opens separate FFmpeg camera readers for live video,
live audio and recording. Its HomeKit features are not evidence that a shared-source
replacement works. Production instrumentation added first RTP/IDR timing and recording
delivery outcomes (`end_sent`, `cancelled`, `source_unavailable`, `write_error`).
`end_sent` is a successful write, not confirmed iCloud storage. The process sampler in
`scripts/ingest_baseline.py` avoids command lines/environment values; process counts
are not RTSP session counts and summed RSS may double-count shared pages.

The unused C-worker draft (`native/`, `native-media.nix`) has been removed and
`src/ingest.rs` replaced by a Rust libavformat acquisition boundary. It is compiled
as application library code, but not wired into `src/main.rs` or production consumers.
MediaMTX was investigated but not added or launched. No production shared-ingest or
Apple acceptance claim follows from these steps.

### Rust acquisition increment

With user-confirmed camera availability, an opt-in read-only check against
**10.76.14.10**, main encoder, exercised `ffmpeg-sys-next` **9.1.1** linked to the
repository's Nix FFmpeg **9.0.2** on Darwin arm64. A five-second read returned **320
packets**, **one missing PTS/DTS**, and **four packets with public RTCP SR side data**.
Descriptors were H.264 **1920×1080, 1/90000**, and AAC **48-kHz mono, 1/48000**.
No clocks were repaired/rebased, encoder settings written, or captured media retained.
This does not establish sender-report cadence or A/V synchronization.

The first run exposed premature packet destruction; after fixing ownership transfer,
the check passed, including cross-thread cancellation (under two seconds), sticky
terminal notification, and packet/codec metadata access after input close. The
opt-in check lives in `tests/camera_acquisition.rs`; it does not run in ordinary tests.
Native diagnostics are suppressed before library I/O because FFmpeg can log URLs;
Rust errors carry only operation/code or typed lifecycle reasons.

`cargo check --all-targets` and `nix build .#amcrust --no-link` passed. The dev shell
now uses the pinned toolchain/media packages directly, avoiding an obsolete chips
NixOS MinIO module import. Still unexercised here: blocked/stalled-camera deadlines,
Rust mux/audio processing, source supervision/fanout, production sockets/connection
counts, Apple playback and privacy behavior. This is not a Rust-only bridge run.

### Shared-source library increment

`tests/shared_camera_source.rs` exercised the real main encoder and Sub Stream 2
through `source::Registry`, without encoder writes or retained media. The final
bounded check completed in about **11.2 seconds** on Darwin arm64:

- Multiple main video subscribers and AAC input shared one acquisition. `lsof`
  inspected this test process's established camera-port sockets: **one main RTSP
  connection**, then **two** with Sub Stream 2 active. This was not process-count inference.
- Two video subscribers received the same packet allocation and generation, starting
  after the initial unclocked packet at admission's checked future IDR. No playback
  decoder or Apple client was used.
- The per-source subscriber cap refused another subscriber. A one-packet slow consumer
  failed with `Budget`; its handed-out packet remained charged after queue retirement.
  A 100-ms residence consumer failed with `Residence`. Healthy consumers continued.
- Disable revoked both queued and held deliveries before returning from reader cleanup;
  a prior `Budget` cause survived the later disable. Re-enable opened a new generation.
- Owner URL replacement retired the main generation while Sub Stream 2 continued.
  Dropping the final main subscriber stopped its reader. Repeated shutdown joined the
  owners; logical byte charges returned to zero after remaining deliveries were dropped.
- The main source reported **three generations/open attempts**, **zero source faults**
  and **two skipped initial unclocked packets** in that final run. Counts varied slightly
  across successful runs; none of these samples establishes a missing-clock repair.

The source layer implements a capped retry/backoff circuit, but this check did not
induce camera disconnects, stalls, authentication failures or actual encoder changes.
It did not evaluate clock rollback/large jumps, RTCP cadence, AAC history, muxing,
Opus, copied recording output, HomeKit privacy, decoded playback or production load.
Library reset/disable is not evidence that the unchanged HomeKit paths are fenced.

All-target type-checking, `nix build .#amcrust --no-link`, and the existing **44
application Rust tests** passed. Strict dependency-wide Clippy was blocked by existing
`hap` warnings; `cargo clippy --lib --no-deps -- -D warnings` passed for the application
library.
No new synthetic camera/clock proof harness was added.

### Rust packet-copy output increment

`tests/camera_media_output.rs` consumed the real main encoder through the Rust source
registry and exercised `media::rtp` and `media::fmp4`. FFmpeg/ffprobe subprocesses were
**downstream diagnostic decoders/demuxers only**, with no camera credentials or direct
camera reads. Media stayed in memory/pipes/loopback UDP; no capture was retained and
no encoder settings were changed. This was not the production HomeKit bridge.

The final roughly **11-second** run included a six-second admitted mux window:

- H.264/AAC fMP4 accepted **1,296,340 bytes** into the Rust-owned AVIO callback and
  retained **91 video / 283 AAC packets**. FFmpeg decoded all **91 video frames** and
  the selected AAC track without diagnostic errors. Video-only muxing decoded the
  same video count. Input payload hashes, PTS/DTS and flags were unchanged by muxing.
- Demuxed PTS/DTS exactly matched the one common-origin translation for both tracks.
  **90 video / 282 AAC packet durations differed** from source-declared durations.
  The camera's nonuniform DTS steps disagree with its declared durations; FFmpeg's
  MP4 sample timeline derives durations from DTS. This is an outstanding presentation
  policy issue, not an implicit tolerance, continuous-decoder PCM comparison or HSV pass.
- Two independent plaintext RTP viewers used **400-/1200-byte** packet limits and
  different SSRC/payload settings. Each FFmpeg receiver decoded **30 frames**, while
  packetizers constructed **3399 / 1148 datagrams** from 100 source frames each.
  Sequence continuity, packet limits and one AU marker were checked. Counts vary with
  real scene content; no first-picture latency or Home app acceptance was measured.
- Real admitted packets exercised partial-AU abandonment, foreign-subscription refusal,
  aggregate job admission and input-byte refusal. Finished copied chunks retained their
  reservations, rejected access after cancellation or source disable, and released
  reservations when dropped. These were not asynchronous HDS backpressure/write checks.

An earlier decoder check reported duplicate output ticks because FFmpeg's default
null-output clock quantized irregular camera video timestamps. The diagnostic now
uses the demux time base and passthrough frame timing; encoded source timing is not
rewritten. An initial 40-frame RTP sample timed out during receiver discovery; the
check now sends 100 frames through a second IDR. Neither change establishes live-start
latency or waives the recorded container-duration difference.

All-target type-checking, `nix build .#amcrust --no-link`, the existing **44 application
Rust tests**, formatting, and strict library-only Clippy passed. Remaining: Rust
AAC-to-Opus processing, common A/V clock
acceptance, recording history/cuts/presentation, session RTCP/SRTP integration, actual
HDS ownership and the single production-reader cutover. Historical C probe retirement
also remains on the TODO; no new C shim or synthetic media gate was introduced.

## Actual camera observation

A bounded run of the native ingest reader connected to **10.76.14.10**, main encoder
(subtype 0), with credentials passed privately. It read 125 framed messages and stopped;
no media capture was retained. This was a reader diagnostic, not a run of the proposed
Rust-only bridge or an Apple playback test. The physical model was not independently
identified by that diagnostic.

Observed descriptor: H.264 High, **1920×1080**, time base **1/90000**, reported
`video_delay=0`; AAC-LC, **48-kHz mono**, time base **1/48000**.

- The first video key packet had missing PTS/DTS. The next sampled video timestamp
  was 5400 while declared packet duration was 6000.
- Initial AAC PTS/DTS included −16, 1008, 1632, 2640 and 3648 with declared duration
  1024: clock steps were not uniformly 1024.
- Sender-report metadata was present, but this sample did not establish report cadence,
  long-term drift, trustworthy A/V mapping or usable missing-clock repair.

**Consequence:** strict contiguous-clock assumptions from the fixtures cannot simply
be promoted to production. The next implementation must handle and evaluate the real
camera's timing, not hide it with invented track origins. The current production code
already contains wall-clock/packet-order workarounds; those are not synchronization
proofs for the replacement.

## Completed experimental work (historical, not rollout gates)

The experiments used Python orchestration and small C wrappers around FFmpeg, usually
FFmpeg 9.0.2 on Darwin arm64. They established useful contracts but did not implement
the final Rust architecture. **Do not schedule more synthetic testing for the cutover.**

| Area | Established within the exercised cases | Important limit / implementation location |
|---|---|---|
| Container choice | NUT fanout decoded, but lost initial B-frame DTS and changed AAC metadata | NUT was rejected as the shared packet contract; `scripts/ingest_probe.py` |
| Packet preservation | Native wrappers preserved tested payloads, clocks, flags, codec parameters and side data; bounded AMC2 binary framing replaced hex payloads | Same-library experimental ABI, not required Rust IPC; `probes/`, `scripts/packet_ingest_probe.py`, `packet_wire.py` |
| Shared metadata | One immutable parse delivered to multiple independent consumers; original source identity/ordinal/sequence guarded | Python prototype, not universal zero-copy; `packet_ingest_probe.py`, `test_shared_metadata.py` |
| Source lifecycle | Disconnect, silent PLAY/socket stall, auth refusal, clock rollback and codec changes retired generations | Loopback scenarios, not broad camera interoperability; `rtsp_ingest_probe.py`, `rtsp_fixture.py` |
| Clock evidence | Early common-epoch SRs, wrap, delayed audio and bounded SR loss reached exact packet-copy A/V output; stale/contradictory evidence refused | No generic late-SR or missing reordered-DTS recovery; `rtsp_clock.py`, `rtsp_clock_probe.py`, `h264_reorder.py` |
| Late admission | Actual current IDR plus matching SPS/PPS; late video independently decoded as source frames 15–29 | Narrow Annex-B Main/High progressive zero-reorder subset; `shared_admission.py` |
| Consumer ownership | Independent byte/count/media-span limits, in-flight charges, original-age expiry, terminal wakeups and first-cause preservation | Cooperative aliases remain readable if escaped; `subscriber_budget.py`, `admission_churn_probe.py` |
| AAC history | Timestamp-selected whole AUs, owned copies, shared pins charged after cache eviction, history-then-live leases | Coverage is not decoder-state equality; `audio_window.py`, `audio_history.py`, `late_audio_probe.py` |
| Paced shared acquisition | Native fanout/history handoffs survived stalls, held prefixes, configuration refusal, early EOF and cancellation | Serial experimental lifecycle, not a Rust production registry; `native_fanout_probe.py`, `native_history_probe.py` |
| Recording cut | Cut at last consumed packet while source continued; newer queued packets excluded; retirement before transfer denied private end | Not duration-driven cuts or output authorization; `recording_cut.py`, `recording_cut_probe.py` |
| Copied output | One-use input/mux/file/delivery ownership, timeout/cancellation, interrupted spawn and repeated-cancellation cleanup | Per-job bounds, cooperative callbacks, not HDS/cloud acceptance; `recording_output.py`, `recording_output_probe.py` |
| Clip presentation | One exact common-origin shift preserved encoded media and relative timing; actual front edits/decoder skip behavior inspected | No production Apple policy or tail-trim proof; `clip_presentation.py`, `presentation_probe.py` |

Clock repair permission was deliberately narrow: explicit matching SPS/PPS, progressive
8-bit 4:2:0, supported POC, zero-reorder VUI and consistent timing—not profile name or
`video_delay=0` alone. Late first SRs exposed audio PTS 0 where the sender schedule
required 250 ms, so merely observing a number did not prove a shared epoch. Missing
reordered DTS, unsupported representations and stale SRs remained refusal cases.

## Media boundaries and presentation

**Finite packet-copy mux:** tested no-B 48-kHz mono and B-frame 32-kHz stereo inputs
preserved packet timing/payloads and decoded video. One finite file retained **384
terminal AAC samples (8 ms)** beyond the exact source PCM prefix. An independent
FFmpeg copy did the same. Source-declared end padding is not the same problem as a
continuous camera clip ending inside an AAC AU (`scripts/av_output_probe.py`).

**Late audio:** taking only the packet-order suffix at an IDR left **3200/3936-sample
(66.7/82-ms) audio gaps** in aligned/delayed fixtures. Timestamp-selected covering AUs
restored packet coverage. An extra preceding AU did not universally reconstruct
continuous PCM: default perceptual-noise-substitution (PNS) output still differed even
with 1/2/4/8 preceding AUs; the PNS-disabled fixture matched after one. No universal
minimum history, audibility conclusion or equality tolerance was established.

**Owned history and cuts:** historical/live handoff preserved original counters;
only private sink counters/end were rebased. A later 20-second paced primary published
**300 video / 926 AAC packets**, delivering **1226 early / 1177 history packets** exactly.
Peak charged history was **16 bodies / 6769 bytes** including evicted-but-pinned data.
Managed frame/range/body owners cleared after each exercised generation; this is not
zero heap/RSS or data erasure. A real-time recording-cut experiment selected **9 video
frames (15–23) + 30 AAC AUs**, then allowed **99 further source packets** in each
successful case. Source EOF was never promoted to a successful recording end.

**Concurrent output:** seven modes covered success, write/finish cancellation,
backpressure timeout, receiver error, file-size refusal and source EOF during a pending
write. Successful delivery was **28082 bytes / seven chunks** while source progress
advanced **94→105 packets** during delivery/decoder checks. Refused writes could already
have accepted 4096 bytes; finish cancellation could occur after the full file was
accepted. Cancellation cannot undo those effects. The prototype used a native
`RLIMIT_FSIZE` wrapper and deterministic process/file cleanup; Rust must own equivalent
limits appropriate to its chosen runtime, not assume those wrappers are production.

**Common-origin presentation:** three fixtures compared exact-clock baseline with
six alternatives. All retained encoded packet hashes/counts/durations and decoded
video. The existing mux's **720000-Hz movie timescale** represented video/audio offsets
exactly; no independent track zeroing or rounded shift was allowed.

| Policy | Observed presentation | PCM / unresolved boundary |
|---|---|---|
| Earliest selected timestamp | AAC starts at zero; video follows by 1184 or 1920 audio samples | All **30720 samples** match the fresh whole-window control; leading audio remains |
| First video IDR | Video starts at zero; AAC begins at −1184/−1920 ticks; actual `elst` media time and `Skip Samples` agree | FFmpeg emits **29536/28800 samples**, exactly the fresh PCM slice from video start; 30 encoded AAC AUs remain |
| Continuous-decoder comparison | Default-PNS Main/High still differ from the video boundary | **662/690 differing samples**, max s16 error **1**; PNS-disabled control exact, not a generic preroll policy |
| Clip end | Main whole-AU audio still extends beyond video | **736 samples / 23⁄1500 s** tail; aligned High ended with video. Front-edit success proves no end trim |

These results are specific FFmpeg decoder/container controls, not Apple acceptance.
Do not trim, pad, retime or waive unexplained differences merely to make a comparison
pass. Presentation, packet retention, decoder history and recording completion are
separate requirements.

## Resource findings without a production savings claim

A matched six-second synthetic acquisition comparison at eight consumers observed:

| Metric | One shared acquisition | Eight independent acquisitions |
|---|---:|---:|
| Native CPU | 0.0693–0.0700 s | 0.5269–0.5287 s |
| Publisher CPU | 0.3389–0.3459 s | 1.1050–1.1103 s |
| Largest sampled worker RSS sum | 17.42–17.75 MiB | 141.41–141.45 MiB |

These were two opposite-order trials, not production FFmpeg/HomeKit workloads. CPU
scopes were separate; input generation was excluded. Sampled RSS sums can double-count
shared pages and miss peaks; individual lifetime RSS high-waters were not summed.
A 20-second eight-consumer extension also passed, but was not a long soak or leak proof.

Earlier 100000-packet fanout used opaque, nondecodable transport stubs. It and the
500-generation small-payload churn run cleared registered owners, not all allocations.
A 500-generation large-payload attempt timed out and was not credited. Lower AMC2 wire
size (~48% in two fixtures) and fewer parses are not standalone CPU/RSS savings claims.

## Retained artifacts and verification boundary

| Numeric record | Contents |
|---|---|
| [native-resource-results.json](native-resource-results.json) | Matched workload fingerprints and scoped acquisition CPU/RSS |
| [late-audio-results.json](late-audio-results.json) | Late AAC, selected windows, decoder differences and owned-history results |
| [native-history-results.json](native-history-results.json) | Paced history lifecycle, eight-/twenty-second primaries and faults |
| [recording-cut-results.json](recording-cut-results.json) | Cut boundaries, continued acquisition, packet/PCM observations |
| [recording-output-results.json](recording-output-results.json) | Concurrent delivery, accepted versus acknowledged bytes and cleanup |
| [clip-presentation-results.json](clip-presentation-results.json) | Exact translated clocks, edit lists, skip signaling and PCM differences |

The last completed experimental milestone (`208540c2`) reported full opt-in Nix
integrations, **215 Python tests**, **61 Rust tests/doctests** and no bounded-review
findings. That is historical evidence for the pre-cutover code, **not validation of
the interrupted C/Rust draft or the planned Rust replacement**. This consolidation
does not rerun synthetic media tests or launch a camera session.

Still unestablished: complete Rust cutover, required real-camera clock handling,
aggregate resource limits, copied-output privacy in the actual HDS path, real transport
backpressure, end-presentation behavior, matched production savings and both camera
models' Apple live/HSV acceptance. Those tasks live only in [TODO](todo.md).
