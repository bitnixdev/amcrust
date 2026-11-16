# Consolidated TODO — Rust cutover and real-device acceptance

**This is the only implementation backlog.** [Architecture](architecture.md) fixes
the direction; [Evidence](evidence.md) separates completed experiments from deployment.
Check an item only when its stated result exists. Do not restart a synthetic-proof
milestone or add another parallel plan/proof document.

## Status

- [x] Production startup/delivery metrics and read-only process sampling exist.
- [x] Packet, clock, fanout, history, cut, output and presentation experiments were
  completed within their recorded scope; their useful findings are consolidated.
- [x] A native reader sampled the authorized real camera. This was not a Rust-only
  bridge run, decoded-view acceptance or a production cutover.
- [ ] Rust-only shared acquisition is implemented and connected to production.
- [x] All project-owned C required for the application is replaced; obsolete C is removed.
- [ ] Real-device live/audio/HSV, privacy, recovery and resource acceptance pass.

## 1. Replace the C-based draft with Rust (start here)

- [ ] Choose and pin Rust bindings compatible with the FFmpeg libraries in Nix. Check
  public packet side-data/RTCP metadata, codec parameters, time bases, read interruption,
  muxing and AAC/Opus APIs against the real camera. Use audited Rust FFI where needed;
  no custom C shim, private FFmpeg structs or new relay dependency.
  - [x] Pin `ffmpeg-sys-next = 9.1.1` against Nix FFmpeg 9.0.2; compile public
    packet/RTCP SR, codec-parameter and interrupt APIs and read the real main stream.
  - [x] Exercise Rust-owned packet-copy muxing against the real camera, including
    H.264/AAC and video-only output, downstream decoding and exact translated PTS/DTS.
  - [x] Exercise Rust AAC-LC decode, fixed-ratio resampling and libopus encode against
    real input at 48/24/16/8 kHz, with 20/40/60-ms frames and padding-domain handling.
  - [x] Characterize real sender-report cadence and public packet clock metadata in
    the Rust source owner. Two read-only 60-/20-second generation pairs exposed
    report-boundary AAC jumps, variable duration debt and SR/PRFT provenance (Evidence).
  - [ ] Settle the observed clock contradictions and recording presentation policy.
    AAC duration debt reached 30–73 ms at the sampled window ends; report-derived
    PRFT is not an independent capture clock. Codec success and a common-looking
    epoch do not establish a usable continuous transport/presentation clock.
- [x] Replace the unfinished `src/ingest.rs` worker/IPC implementation with Rust-owned
  library acquisition. Reuse sound contracts, not its standalone-C process assumptions.
- [x] Define owned packet/configuration/error types, reference lifetimes, thread-safety
  boundary and interruption/shutdown behavior before exposing subscriptions. Redact
  credential-bearing URLs in native-library diagnostics as well as Rust logs.
  The blocking reader is thread-bound; immutable packet/configuration owners cross
  threads. Deadlines and sticky cancellation interrupt library I/O; Drop closes it.
  `src/source/` now adds joined supervision and bounded subscriptions. Production
  injection and copied-output/transport privacy are still section 2/3 work.
- [x] Complete every applicable row below; update Cargo/Nix and remove stale entrypoints.

| Project C / related scaffolding | Required Rust replacement or removal |
|---|---|
| `native/ingest.c` | **Removed.** `src/ingest.rs` owns libavformat open/read/interrupt/close, codec metadata and packet/side-data lifetimes; no production consumer is connected yet |
| `probes/ingest_worker.c` | **Removed.** Retired the experimental fixture exporter; acquisition lives in the Rust library |
| `native/output.c` | **Unused C draft removed.** `src/media/rtp.rs` packetizes H.264; `src/media/fmp4.rs` owns finite packet-copy mux jobs. Production transport/recording integration remains |
| `probes/mux_worker.c` | **Removed.** Rust muxing copies public codec parameters directly and owns packets/AVIO cleanup; the experimental sink and launchers are retired |
| `probes/packet_wire.h` | **Removed.** Typed Rust packets replace worker IPC; retained numeric JSON evidence does not need an AMC2 parser |
| `nix/packages/native-media.nix` | **Removed.** Application package and dev shell now supply FFmpeg libraries, pkg-config and bindgen dependencies |
| `nix/packages/ingest-worker-probe.nix` | **Removed.** No probe package or direct json-c/probe-only dependency remains; no replacement synthetic rollout gate |
| Python worker launchers / unused AMC2 scaffolding | **Removed.** Retired 52 experimental Python files; preserved findings/numeric records and the independent `ingest_baseline.py` workload sampler with its two existing checks |

**Exit:** required media behavior is Rust application code using upstream libraries;
no project-owned `.c`/`.h` is needed to build or run amcrust. Removing obsolete
probe-only code is preferable to porting an unused IPC architecture just to keep it.
This does not require rewriting FFmpeg itself. The project-owned C/scaffolding cleanup
is complete; the direct FFmpeg camera readers and unresolved media policies still need
the production integration below. Removing probes is not the acquisition cutover.

## 2. Make shared acquisition the production owner

- [ ] Registry: one acquisition per active camera encoder, shared by all consumers
  needing it. Main audio/video/recording share one main source; Sub Stream 2 is separate.
  - [x] Implement the encoder-keyed Rust registry and non-secret source handles.
    Real library consumers shared one main RTSP socket; adding Sub Stream 2 used two.
  - [ ] Inject that registry into the production consumers (section 3).
- [ ] Supervisor: startup/read deadlines, bounded retry/backoff, generation reset,
  parameter changes, idle stop, cancellation and deterministic resource cleanup.
  - [x] Own one blocking reader job per active source; revoke before interrupt/join;
    idle-stop, disable/re-enable, owner URL replacement and repeatable joined shutdown.
    Faults retire leases; subsequent subscriptions use capped exponential backoff and
    a consecutive-failure circuit breaker, never splice a reconnect into old leases.
  - [ ] Exercise real disconnect/stall/retry and changing camera parameters; connect
    source recovery/configuration to production owners.
- [ ] Device clocks: handle the observed missing first video PTS/DTS and irregular
  clock steps explicitly. Measure real RTCP cadence and A/V behavior; do not paste in
  fixture deadlines, invent reordered DTS, independently zero tracks or silently keep
  the old wall-clock/packet-order policies as though they were source-clock evidence.
  - [x] Skip initial unclocked packets without repair; video waits for a subsequent
    timestamped actual IDR. Preserve irregular signed clocks; later missing or
    unexplained non-increasing DTS retires the generation. Classified bounded AAC
    SR remapping now has the explicit audio-only exception below, not a timestamp
    rewrite or A/V proof.
  - [x] Expose bounded generation-scoped SR/PRFT and duration-debt observations.
    Malformed public metadata, changed report SSRC and non-advancing/ambiguous report
    clocks are refused; retired observations are never reused for a new reader.
    Report age is diagnostic only: no fixture-derived freshness deadline or clock
    correction is imposed. A bounded first-failure record now distinguishes metadata
    refusal from missing/non-increasing media timestamps and survives retirement.
    Common A/V timing and priming acceptance remain open.
- [ ] Fanout: current-IDR/SPS/PPS admission, independent bounded subscriptions, charged
  outstanding deliveries, residence limits and first-cause terminal notification.
  - [x] Implement Annex-B single-SPS/PPS slice-reference admission, no GOP replay,
    packet/count/byte/per-track-span limits including handed-out deliveries, residence
    expiry and first-cause terminal wakeups. Real slow/expired consumers failed locally.
  - [ ] Integrate with transports and recording history; verify decoded-view behavior.
    AudioVideo admission alone does not provide covering AAC history for recording.
- [ ] Configuration/privacy: apply changes through the owner, retire affected generations
  before reopening, revoke cached/pinned/copied output, and stop transport work promptly.
  - [x] Retire source grants and clear queued deliveries before awaited cleanup;
    held deliveries reject access while retaining charges until dropped. Main source
    replacement left Sub Stream 2 running in the real check.
  - [ ] Fence actual transport writes and recording copies; borrowed/escaped data
    cannot be retracted by this cooperative library layer. Session-owned live UDP
    writes now have a revoke-before-join fence; coupling it to source-generation
    retirement and fencing recording copies remain open.
- [ ] Admission/resource limits: per-source, per-subscriber and aggregate job/byte caps;
  expose active source/subscriber counts, queue age/bytes, faults and recovery metrics.
  - [x] Bound registry entries, subscriber slots, handoff/descriptor/delivery byte
    charges per source and registry; expose typed source snapshots and aggregate bytes.
    Charges are conservative logical ownership accounting, not allocator/RSS limits.
  - [ ] Connect Prometheus metrics and recording-job/copy/history budgets; evaluate
    aggregate pressure and matched real production workloads.

## 3. Wire consumers and remove direct camera readers

- [ ] `src/main.rs`: construct and inject the shared source service. Only acquisition
  receives RTSP credentials/URLs; downstream callers receive source/subscription handles.
- [ ] `src/stream.rs`: remove direct RTSP opens for video and audio. Feed encoded H.264
  into Rust RTP packetization with the negotiated MTU/payload type and fresh per-viewer
  transport state. Keep existing `src/srtp.rs` encryption/control integration working.
  - [x] Implement per-viewer single-NAL/FU-A packetization with independent sequence,
    clock origin, SSRC/payload/MTU configuration, current in-band SPS/PPS and AU marker.
    Two real-source plaintext RTP receivers decoded at 400-/1200-byte packet limits.
  - [ ] Connect session transport, RTCP and SRTP/SRTCP; reserve authentication overhead
    from the negotiated MTU and verify Home app playback.
    - [x] Route existing production video/audio adapters through the shared Rust
      secure UDP transport; remove their inline encryption/socket-write path. Bound
      protected datagrams, reserve SRTP overhead, authenticate/replay-check feedback
      and revoke all affected tracks before awaited media teardown.
    - [x] Exercise real-source Rust H.264 directly through that transport: two FFmpeg
      SRTP receivers decode 30 frames each at 400-/1200-byte protected datagram limits.
    - [x] Build outbound SR/SDES in Rust using kernel-accepted RTP packet/payload
      counts and a fresh session CNAME shared by audio/video. Replace verbatim
      forwarding of FFmpeg reports; preserve only their explicit clock pair/cadence
      in the legacy adapter. Validate compound framing and propagate terminal BYE.
    - [x] Consume authenticated receiver feedback in Rust, removing the UDP return
      path to FFmpeg. Bound and validate RR/SR, SDES, PLI/FIR/NACK and BYE; retain only
      fixed-size report/counter state and expose fixed-label metrics. Requests do not
      imply retransmission or forced IDR support. Peer BYE retires its track.
    - [x] Replace split live child/proxy maps with joined media ownership. Native
      producer exit fences transport; track termination kills/reaps its producer and
      joins its diagnostic drain. Real camera RR/PLI/BYE closed the video producer
      without a HAP stop request; local checks cover explicit stop and producer exit.
    - [ ] Replace the FFmpeg-facing adapters with Rust media producers, a qualified
      source-to-NTP mapping and report scheduling, then
      verify Home app playback. Report formatting alone does not supply a source
      clock, pacing, source-generation fence or audio priming policy.
- [ ] Live audio: use shared AAC input; own FFmpeg-library decode/Opus encode in Rust.
  Pool only compatible negotiated settings and isolate encoder/subscriber failures.
  - [x] Implement thread-bound AAC-LC -> S16 resampler/FIFO -> libopus processing,
    with exact negotiated settings, bounded job/output ownership and explicit finish.
    Real inputs exercised four configurations, local encoder failure, cancellation
    and source retirement without stopping other processors.
  - [x] Preserve original clocks separately from private codec sample ordinals;
    retain contradiction evidence and refuse unsupported source-timestamp mapping.
    Convert Opus skip/discard metadata exactly from encoder-rate to 48-kHz units.
  - [ ] Qualify the selected clock/priming policy for real live A/V. Original AAC PTS
    are NOT relabeled as continuous; source observations and mismatch counts remain.
    The decision, alternatives and wrong-choice symptoms are in Architecture.
    - [x] Add explicit source-anchored decoded-sample progression for 48-kHz AAC input,
      retaining strict default behavior. Require SR/PRFT admission, guard phase at
      100 ms and SR-bearing-packet age at 15 s, and revoke only the failed processor.
      These are provisional refusal budgets, not accepted lip-sync tolerances.
    - [x] Expose covered audible intervals and effective OpusHead/packet skip/discard
      requirements; refuse raw timestamp mapping for packets needing trims. Check
      actual decoded sample counts and continuous candidate output on the camera.
    - [ ] Run a sustained qualification window (30 minutes or first refusal), recording
      phase slope and budget trips without automatic reanchoring/restarting. Compare
      actual first-onset and sustained A/V sync on paired clients, including stalls,
      SR boundaries and late joins. A persistent slope triggers clock-recovery review,
      not a larger phase limit. Keep the candidate opt-in until acceptance exists.
      - [x] Attempt the 30-minute-or-first-refusal check with unchanged budgets:
        **failed with `Source(Clock)` after 34.33 s total**, not a candidate phase trip.
        Last periodic phase was 688 samples/14.33 ms. This is NOT qualification.
        A separate instrumented 60-second diagnostic passed; it does not replace the
        failed attempt or identify its offending packet retroactively (Evidence).
      - [x] Capture/classify a subsequent source-clock refusal with first-failure
        metadata. The user-requested real-camera retry failed after 200.19 s total:
        AAC DTS went backward 96 ticks/2 ms on an SR-bearing packet; admission
        correctly retired the generation. One acquisition, no candidate budget trip.
      - [x] Classify SR-boundary AAC rollback using adjacent public metadata and the
        pinned FFmpeg mapping: reconstructed RTP advanced while mapped DTS reversed,
        with exact 170-ms and 152-ms report-mapping shifts. This is dependent metadata,
        not an independent wire capture. The user confirmed the other service was off.
      - [x] Add `Tracks::AudioSampleClock` for bounded, demonstrable AAC SR-remapping
        steps. Preserve original timestamps; revoke strict audio/AudioVideo locally,
        retain video, and leave the processor's ±100-ms/15-s limits unchanged.
        A mapping step may span the window's 200-ms diameter; it does not authorize
        either endpoint beyond 100 ms. Unknown/reset/missing timing on active tracks
        still retires the generation; oversized explained AAC changes now quarantine
        audio rather than granting continuation.
      - [x] Observe bounded remapping on live input: sequence 5859 repeated AAC DTS
        while reconstructed RTP advanced 1152 ticks, with a matching −1152-tick SR
        shift. Strict audio revoked; candidate audio and video continued in the same
        generation until a later 276-ms shift exceeded the bound. This establishes
        live subscription isolation, not sustained clock or physical playback acceptance.
      - [x] Separate reconstructed RTP duration debt from SR mapping and unexplained
        residuals in constant-space source diagnostics. A real 60-second endpoint
        showed 1456 ticks of RTP debt + 144 ticks of report shift + 16 unexplained
        ticks = 1616 ticks of candidate phase. SR updates alone do not explain this
        window; the values are not independent capture/oscillator measurements.
      - [x] Quarantine all audio-bearing subscriptions on an oversized explained AAC
        mapping change, preserving healthy video and refusing new audio until a new
        generation. Keep original timestamps, endpoint limits and first-cause revocation.
      - [ ] Exercise quarantine, continued video and explicit new-generation recovery
        on an actual oversized audio event. First hardware check instead stopped on
        a 75-ms video DTS rollback; second timed out after 300 s without the event.
        Neither is a successful quarantine exercise.
      - [ ] Handle/qualify video timing independently: the real H.264 rollback was on
        an SR-bearing packet but the AAC-only reconstruction does not classify video.
        Do not suppress video faults or infer raw video continuity from that coincidence.
      - [ ] Resolve sustained drift/clock recovery: the candidate independently hit
        4816 samples/100⅓ ms at 97.36 s. Do not increase its phase budget or reanchor
        to conceal this. Choose/qualify the next clock policy before production use.
    - [x] Extend the candidate to 8/16/24/48-kHz output and 20/40/60-ms frames.
      Keep all timing/trims in the 48-kHz decoder domain; explicitly trim only a
      sub-encoder-sample finite excess. Undercoverage is refused, not padded or
      acknowledged as a complete interval. Real negotiated-rate checks exercise both
      covered endpoints and a one-tick shortfall, with native metadata unchanged.
    - [ ] Implement the live receiver's preroll/trim and late-join decoder-history
      contract. RTP does not carry OpusHead/skip side data automatically. Retain needed
      packets; do not hide initial clipping, noise or end padding by dropping them.
  - [ ] Share processing only for matching source generation and negotiated settings;
    the current audio pool bounds jobs, but does not yet deduplicate encoders.
    Connect audio RTP/RTCP/SRTP and production lifecycle/metrics. New Opus timing is
    already in 48-kHz units: do not apply the legacy FFmpeg proxy's rate rescaling
    again when replacing that reader/transport path.
- [ ] `src/hsv/recording.rs` / `src/hsv/fmp4.rs`: remove the recording camera reader;
  consume generation-bound shared packets, mux through FFmpeg bindings and preserve
  correct initialization, prebuffer, actual-IDR start, historical/live order and cuts.
  - [x] Implement a thread-bound finite Rust libavformat muxer, full parameter/side-data
    copying, deterministic stream selection and exact common-origin translation.
  - [ ] Add covering AAC/history selection and production fragment/prebuffer/HDS wiring.
    Current mux input is an admitted live suffix, not a completed recording-cut policy.
- [ ] Recording output: keep ownership through muxing and HDS delivery, including
  pending writes, timeout, cancellation, repeated cancellation and temporary storage.
  Separate accepted bytes, local finish/end-write and confirmed user-visible recording.
  - [x] Bound finite mux input/output bytes, packets, chunks, lifetime and aggregate
    job reservations; keep reservations and revocable grants with copied output chunks.
    Real-source checks exercised cancellation, source revocation and local limits.
  - [ ] Carry ownership through actual asynchronous HDS writes/completion and recording
    history. Local mux finish and accepted AVIO bytes do not acknowledge a recording.
- [ ] `src/accessory.rs`: connect HomeKit privacy/configuration transitions to source,
  session and output ownership; ensure disabling recording alone does not break live view.
- [ ] `apply-settings --write`: exercise the replacement production recording path,
  not an old direct-reader diagnostic that gives misleading acceptance results.
- [ ] Remove obsolete commands, structures, worker selection and build/runtime deps.
  No feature flag, fallback, duplicate implementation or per-consumer direct RTSP reader.

**Exit:** one coherent production path covers live video, live audio and recording.
Do not mark this done just because an isolated Rust reader can obtain packets.

## 4. Run and evaluate the real replacement (alongside integration)

The authorized target and command are:

```sh
cargo run --release -- --name amcrust-test --host 10.76.14.10
```

Use credentials already supplied through the environment/`.env`; never copy secrets,
RTSP URLs containing credentials, setup PINs or captured media into tracked evidence.
Startup/encoder writes are authorized. The accessory name isolates pairing state,
not the physical camera: coordinate any existing instance controlling that encoder.

- [ ] Launch the replacement, report the actual source/backend in use, and verify
  connection count on the real camera/socket path—not merely process count.
- [ ] Ask the user to pair/open `amcrust-test`: verify first decoded picture, sustained
  live video, audio and lip sync, simultaneous viewers and the second live resolution.
- [ ] Ask the user to trigger/evaluate HSV: pre-event content, clip beginning/end,
  audio, playback/seek, successive events and an actual recording appearing in Home.
- [ ] Settle start/end presentation with those devices. Characterize the remaining
  whole-AU tail and decoder-state behavior; do not assume front edits solve end trims
  or turn residual sample differences into an implicit tolerance.
- [ ] Exercise stopped/slow viewers, output backpressure, camera disconnect/stall,
  reconnect, encoder change, repeated start/stop and HomeKit privacy disable/re-enable.
  Verify another subscriber/camera survives local failures; check for leaked work.
- [ ] Compare matched real workloads: camera connections, processes, CPU/RSS including
  children, first RTP/IDR **and separately** first decoded picture, recovery and HSV.
  Use `scripts/ingest_baseline.py` where useful; historical synthetic savings are not
  production measurements. Do not run old/new acquisition together as a shipped mode.
- [ ] Repeat acceptance on both `IP8M-2696E-AI` and `IP8M-2796E-AI` when available,
  using Apple OS 27 clients. Record which model/client/path was actually exercised.
- [ ] Update [Evidence](evidence.md) with concise real outcomes and outstanding failures;
  amend the existing architecture/TODO rather than creating another milestone document.

## 5. Close remaining behavior and packaging work

- [ ] Aggregate limits and overlapping/duration-driven recording cuts behave correctly
  under real load, not merely within one bounded diagnostic job.
- [ ] Required camera clock/codec representations are supported or explicitly refused
  with useful diagnostics; no unsupported case receives guessed timing/admission.
- [ ] Nix package/service and the documented Cargo workflow provide the Rust library
  dependencies, without project C workers or system-wide tool installation.
  - [x] Check in `.envrc` so direnv loads the pinned native build dependencies; ignore
    its local cache and document approval plus hook-free Cargo commands.
- [ ] README accurately describes the implemented pipeline and operating requirements.
  Remove obsolete research entrypoints when no longer useful; retain compact findings
  and raw numeric artifacts only where they still explain a decision.

**Definition of done:** Rust-owned shared acquisition is the only production camera
reader; live/audio/HSV work on real devices with independent transport state, bounded
owned resources, privacy/recovery behavior and recorded user acceptance. The C/relay
detours and legacy direct-reader implementation are gone—not hidden behind a switch.
