# Consolidated TODO — Rust cutover and real-device acceptance

**This is the only implementation backlog.** [Architecture](architecture.md) fixes
the direction; [Evidence](evidence.md) separates completed experiments from deployment.
Check an item only when its stated result exists. Do not restart a synthetic-proof
milestone or add another parallel plan/proof document.

## Execution guidelines

- Work in larger, end-to-end chunks with a meaningful operational result, rather
  than treating each prerequisite or intermediate edit as a release checkpoint.
- Settle the design and explicit limits once; reopen decisions when new evidence
  warrants it, not through repeated speculative edge-case analysis.
- During implementation, use targeted compilation and focused regression tests
  where a mistake would invalidate the next layer, especially codec timing,
  ownership and privacy fences. Do not defer all testing until the end.
- At the milestone boundary, batch the full relevant suites, one coordinated
  authorized camera acceptance run, the Nix package build and one bounded review.
  Repeat expensive checks only for relevant changes or failures.
- Batch architecture/evidence/checklist updates and nonblocking cleanup near the
  end of the chunk, then commit the coherent result. Keep cleanup needed for
  correctness or safe integration within the implementation itself.
- Keep progress updates to meaningful findings, decisions or blockers. Avoid
  repeated planning, narration and checkpoint overhead without a concrete benefit.
- Larger batches do not relax camera-access authorization, safety limits or evidence
  requirements. Code-path completion, deployment and Apple acceptance stay distinct.

## Status

- [x] Production startup/delivery metrics and read-only process sampling exist.
- [x] Packet, clock, fanout, history, cut, output and presentation experiments were
  completed within their recorded scope; their useful findings are consolidated.
- [x] A native reader sampled the authorized real camera. This was not a Rust-only
  bridge run, decoded-view acceptance or a production cutover.
- [x] Rust-owned shared acquisition is connected to live video/audio and recording;
  no legacy media-reader fallback remains.
- [x] All project-owned C required for the application is replaced; obsolete C is removed.
- [ ] Real-device live/audio/HSV, privacy, recovery and resource acceptance pass.

**Current priority:** operational acceptance and remaining configuration/privacy,
recovery and resource work. Production uses recovered presentation timing and shared
acquisition; camera clocks remain diagnostics, not trusted playback authority.
Unattended recovery across outages/reboots remains required. Do not restart completed
reader cutovers or strict candidate qualification as prerequisites.

## 1. Replace the C-based draft with Rust

- [x] Choose and pin Rust bindings compatible with the FFmpeg libraries in Nix. Check
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
  - [x] Select and implement recovered live/recording presentation, retaining original
    clock contradictions as evidence rather than silently repairing them. SR/PRFT is
    not an independent capture clock. Sustained/Apple presentation acceptance is in
    section 4; choosing this policy does not resolve physical capture timing.
- [x] Replace the unfinished `src/ingest.rs` worker/IPC implementation with Rust-owned
  library acquisition. Reuse sound contracts, not its standalone-C process assumptions.
- [x] Define owned packet/configuration/error types, reference lifetimes, thread-safety
  boundary and interruption/shutdown behavior before exposing subscriptions. Redact
  credential-bearing URLs in native-library diagnostics as well as Rust logs.
  The blocking reader is thread-bound; immutable packet/configuration owners cross
  threads. Deadlines and sticky cancellation interrupt library I/O; Drop closes it.
  `src/source/` supplies joined supervision and bounded subscriptions. Production
  injection and final RTP/HDS write fencing are connected (sections 2/3).
- [x] Complete every applicable row below; update Cargo/Nix and remove stale entrypoints.

| Project C / related scaffolding | Required Rust replacement or removal |
|---|---|
| `native/ingest.c` | **Removed.** `src/ingest.rs` owns libavformat open/read/interrupt/close, codec metadata and packet/side-data lifetimes; all production media consumers are connected |
| `probes/ingest_worker.c` | **Removed.** Retired the experimental fixture exporter; acquisition lives in the Rust library |
| `native/output.c` | **Unused C draft removed.** `src/media/rtp.rs` packetizes H.264; `src/media/fmp4.rs` owns finite packet-copy mux jobs. Production RTP/HDS integration is connected |
| `probes/mux_worker.c` | **Removed.** Rust muxing copies public codec parameters directly and owns packets/AVIO cleanup; the experimental sink and launchers are retired |
| `probes/packet_wire.h` | **Removed.** Typed Rust packets replace worker IPC; retained numeric JSON evidence does not need an AMC2 parser |
| `nix/packages/native-media.nix` | **Removed.** Application package and dev shell now supply FFmpeg libraries, pkg-config and bindgen dependencies |
| `nix/packages/ingest-worker-probe.nix` | **Removed.** No probe package or direct json-c/probe-only dependency remains; no replacement synthetic rollout gate |
| Python worker launchers / unused AMC2 scaffolding | **Removed.** Retired 52 experimental Python files; preserved findings/numeric records and the independent `ingest_baseline.py` workload sampler with its two existing checks |

**Exit:** required media behavior is Rust application code using upstream libraries;
no project-owned `.c`/`.h` is needed to build or run amcrust. Removing obsolete
probe-only code is preferable to porting an unused IPC architecture just to keep it.
This does not require rewriting FFmpeg itself. The project-owned C/scaffolding cleanup
and production media-reader replacement are complete. The retained diagnostics and
upstream FFmpeg libraries are not legacy acquisition fallbacks.

## 2. Make shared acquisition the production owner

- [x] Registry: one acquisition per active camera encoder, shared by all consumers
  needing it. Main audio/video/recording share one main source; Sub Stream 2 is separate.
  - [x] Implement the encoder-keyed Rust registry and non-secret source handles.
    Real library consumers shared one main RTSP socket; adding Sub Stream 2 used two.
  - [x] Inject encoder-keyed sources into production live video; remove video URLs
    from downstream `VideoSource` handles and join the registry at application shutdown.
  - [x] Connect live audio and recording to the same owners (section 3).
- [ ] Supervisor: startup/read deadlines, bounded retry/backoff, generation reset,
  parameter changes, idle stop, cancellation and deterministic resource cleanup.
  - [x] Own one blocking reader job per active source; revoke before interrupt/join;
    idle-stop, disable/re-enable, owner URL replacement and repeatable joined shutdown.
    Faults retire leases; subsequent subscriptions use capped exponential backoff and
    a consecutive-failure circuit breaker, never splice a reconnect into old leases.
  - [x] Replace the permanent breaker latch with timed cooldown and fresh-demand retry.
    Three native RTSP setup failures exercised repeated cooldown without operator
    reset; idle/disabled sources stayed closed and old subscriptions stayed retired.
    A real camera relay check then recovered authenticated IDR output in generation 3
    after two setup failures, without source reset or endpoint replacement. Mid-stream
    outages, physical camera reboot and production recovery are still unqualified.
  - [x] Connect production encoder configuration to source retirement and fresh retry
    state, with live-profile updates and independent recording-output changes.
  - [ ] Exercise real disconnect/stall/retry and approved changing camera parameters;
    qualify controller recovery rather than merely fresh application demand.
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
- [x] Fanout: current-IDR/SPS/PPS admission, independent bounded subscriptions, charged
  outstanding deliveries, residence limits and first-cause terminal notification.
  - [x] Implement Annex-B single-SPS/PPS slice-reference admission, no GOP replay,
    packet/count/byte/per-track-span limits including handed-out deliveries, residence
    expiry and first-cause terminal wakeups. Real slow/expired consumers failed locally.
  - [x] Integrate transports and recording history with real decoded-output checks.
    AudioVideo admission alone is not history: a separate bounded recording owner
    now supplies IDR selection and recovered-AAC preroll for clip-local mux jobs.
- [ ] Configuration/privacy: apply changes through the owner, retire affected generations
  before reopening, revoke cached/pinned/copied output, and stop transport work promptly.
  - [x] Retire source grants and clear queued deliveries before awaited cleanup;
    held deliveries reject access while retaining charges until dropped. Main source
    replacement left Sub Stream 2 running in the real check.
  - [x] Fence actual live transport and copied HDS writes with source/subscription
    retirement plus session/configuration/job gates. Already kernel-accepted bytes
    cannot be retracted. Real fMP4/HDS checks refuse retained output after source disable.
  - [x] Bind library transports to source-generation and subscription retirement gates.
    Real authenticated RTP exercised local cancellation, independent video, source
    disable, copied-RTP/RTCP refusal and fresh-generation isolation. Predicate/syscall
    races and acknowledgement ordering are also covered locally.
- [ ] Admission/resource limits: per-source, per-subscriber and aggregate job/byte caps;
  expose active source/subscriber counts, queue age/bytes, faults and recovery metrics.
  - [x] Bound registry entries, subscriber slots, handoff/descriptor/delivery byte
    charges per source and registry; expose typed source snapshots and aggregate bytes.
    Charges are conservative logical ownership accounting, not allocator/RSS limits.
  - [x] Bound recording history, AAC jobs/outputs, clip jobs, selected input, emitted
    bytes and HDS queues; retain charges with escaped output permissions.
  - [x] Connect fixed-label source snapshots, live Opus/recording AAC/mux pool
    reservations, retained live-owner counts and recording history to production
    Prometheus and health responses. Scrapes report lock contention explicitly.
  - [x] Bound live admission to 16 retained preparations and eight owned session IDs
    across both tracks; bound setup metadata, reject starts after a 30-second preparation
    deadline, and join completed owners before reclaiming slots. Expose caps, capacity
    refusals and expired preparations through health/metrics. These are ceilings, not
    a guarantee of eight A/V viewers; source/codec limits still apply.
  - [x] Publish source opening/acquisition/backoff/cooldown/retirement/mode phases,
    consecutive failures, monotonic remaining delay, bounded last-fault categories and
    circuit entry/reset counters. Validate local native reset/stall, privacy/reset,
    sticky old subscriptions and fresh-demand semantics without changing retry policy.
  - [x] Expose the registry's actual aggregate acquisition byte/slot budgets, retained
    per-source slots and slot-refusal counters through health/Prometheus. Validate
    cross-source pressure, partial-reservation rollback and retained ownership across
    retirement/shutdown. These reuse the existing limits; they are not allocator/RSS caps.
  - [x] Enforce one shared codec/mux ceiling (10 jobs/160 MiB) across live Opus,
    recording AAC and clip muxing in addition to each role limit. Retain both charges
    with escaped job outputs and expose reservations/refusals at the limiting pool.
  - [ ] Complete remaining pipeline accounting and consumer-recovery observability;
    evaluate pressure, actual native RSS and matched real production workloads.
    Per-source charges, history payloads and full-job reservations overlap and must
    not be summed as allocator memory.

## 3. Wire consumers and remove direct camera readers

- [x] `src/main.rs`: inject the same main source into live video/audio and recording.
  Consumer configuration contains no camera URL. A 60-second production A/V plus
  recording check used one acquisition and preserved live owners after recording stop.
- [x] `src/stream.rs`: remove direct RTSP opens for video and audio. Feed encoded H.264
  into Rust RTP packetization with the negotiated MTU/payload type and fresh per-viewer
  transport state. Keep existing `src/srtp.rs` encryption/control integration working.
  - [x] Replace the live audio FFmpeg command/relay with a joined Rust worker consuming
    the shared main source. Give both tracks a common session clock; generate audio
    RTP/RTCP in Rust and remove the legacy timestamp rescaler/report extractor.
    Real 60-second A/V and all twelve negotiated audio formats decoded successfully.
    This does not complete Apple playback, pooling or outage recovery acceptance.
  - [x] Replace the production video FFmpeg reader/RTP command and local video relay
    with a shared-source Rust producer. Own presentation timestamps, SR scheduling,
    feedback and cancellation; protect pending startup and release ended-owner resources.
    Real production checks exercise one source/two owners and independent stop; separate
    presentation-mode receivers decode 30 frames each. HomeKit A/V acceptance is open.
  - [x] Implement per-viewer single-NAL/FU-A packetization with independent sequence,
    clock origin, SSRC/payload/MTU configuration, current in-band SPS/PPS and AU marker.
    Two real-source plaintext RTP receivers decoded at 400-/1200-byte packet limits.
  - [x] Connect session transport, RTCP and SRTP/SRTCP; reserve authentication overhead
    from the negotiated MTU. The user confirmed Home playback and corrected audio;
    measured synchronization and unattended recovery remain acceptance work below.
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
    - [x] Replace the video adapter with explicit host-monotonic presentation and
      current host-NTP/RTP pairs; schedule reports in Rust using the negotiated minimum
      interval. Preserve original camera timestamps and first clock contradictions.
      Reject reordered B slices, corruption and mismatched negotiated dimensions.
    - [ ] Qualify common A/V presentation and recovery, then verify Home app playback.
      The audio adapter has been replaced. Read-time video timestamps alone do not establish
      burst handling, physical synchronization or audio priming.
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
    - [ ] Qualify the recovered presentation policy over sustained A/V, stalls, late
      joins and reconnects, including first-onset and actual paired-client playback.
      Bound continuous resampling and keep source evidence unchanged. The earlier
      source-anchored candidate did NOT qualify; its results below remain historical,
      not a requirement to repeat that failed policy until the camera happens to pass.
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
        to conceal this. The user has now selected recovered presentation timing;
        a separate bounded presentation resampler is now wired into production;
        sustained Apple playback and unattended outage recovery remain unqualified.
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
    Audio RTP/RTCP/SRTP and joined production lifecycle are connected. The removed
    legacy rescaler is not restored. Internal timing stays in 48-kHz units; the
    HomeKit boundary now maps RTP and RTCP together to the negotiated rate.
- [x] `src/hsv/recording.rs` / `src/hsv/fmp4.rs`: remove the recording camera reader;
  consume generation-bound shared packets, mux through FFmpeg bindings and preserve
  correct initialization, prebuffer, actual-IDR start, historical/live order and cuts.
  - [x] Implement a thread-bound finite Rust libavformat muxer, full parameter/side-data
    copying, deterministic stream selection and exact common-origin translation.
  - [x] Add bounded shared history and clip-local initialization, IDR start, at least
    two AAC preroll AUs, atomic snapshot/live handoff and covered whole-AU tail selection.
    Recording AAC is continuously recovered/re-encoded, not original AAC packet-copy.
    Two successive clips each at 32 kHz, 48 kHz and video-only decoded with continuous
    durations and covering tails; this is not Apple priming/seek acceptance.
- [x] Recording output: keep ownership through muxing and HDS delivery, including
  pending writes, timeout, cancellation, repeated cancellation and temporary storage.
  Separate accepted bytes, local finish/end-write and confirmed user-visible recording.
  - [x] Bound finite mux input/output bytes, packets, chunks, lifetime and aggregate
    job reservations; keep reservations and revocable grants with copied output chunks.
    Real-source checks exercised cancellation, source revocation and local limits.
  - [x] Fence actual HDS writes with recording/stream retirement, including copied and
    encrypted bytes. Wake blocked writes; poison truncated-frame connections. Join
    recording producers, diagnostic drains, delivery tasks and HDS server shutdown;
    serialize reconciliation and await recording disable before acknowledging it.
    Real fMP4 bytes exercised HDS authentication and retained-fragment refusal.
  - [x] Bind HDS permissions to Rust source/mux/history grants. Revoke before joining
    history and clip workers; retain join ownership across cancelled waits. Refuse
    incomplete coverage; mark HDS end only after local mux finish. Clean duration/size
    cuts are wired, but automatic Apple continuation is unqualified. Accepted TCP/AVIO
    bytes still do not acknowledge a stored recording.
- [ ] `src/accessory.rs`: complete privacy/configuration behavior across all media.
  - [x] Wire HomeKit Camera Active to live admission, all registered encoder sources
    and recording. Camera-off discards prepared sessions, revokes and joins live/HSV;
    enable needs fresh demand/session setup. Apply persisted privacy before HAP startup.
    Two real off/on cycles authenticated live A/V and refused retained recording output.
  - [x] Retain live owner join handles across cancelled teardown and permanently close
    admission on shutdown; remove the old remove/drain-before-await ownership pattern.
  - [x] Fence snapshot cache/publication and final encrypted HTTP writes with sticky
    epochs across off/on and snapshot-reason changes. Join the refresher on shutdown;
    reject old fallback frames, wake blocked writes and poison partial responses.
    Local encrypted delivery/backpressure checks pass; no new camera access was used.
  - [x] Wire RTP-service Active through persistence and joined live admission/teardown,
    independently of recording/snapshots. Remove the hard-coded always-active value.
    Local characteristic, cancellation, state migration and shared-owner checks pass.
  - [ ] Qualify actual Home camera modes/automations and Apple behavior when source or
    snapshot retirement closes a session mid-response; service-level wiring is not
    deployed client acceptance.
  - [x] Retire the main source and join affected live/recording owners before encoder
    writes; invalidate snapshot epochs, publish refreshed live profiles only on success,
    and propagate settings errors. Failed/cancelled encoder transitions stay blocked
    across privacy toggles until a successful retry. Remove wrong-format substitution.
  - [ ] Qualify approved real encoder transitions and firmware-induced substream resets;
    the transaction/isolation evidence is local and does not establish Apple recovery.
  - [x] Join recording disable/configuration replacement and preserve live subscribers;
    simultaneous production live A/V survived recording stop in the 60-second check.
    Recording output-rate changes no longer reconfigure acquisition audio.
- [x] Route `apply-settings --write`'s probe through the production shared-source
  history/recovered-AAC/mux path. The mutation-bearing CLI was not run in this increment;
  validation remained serial/read-only against the authorized camera.
- [x] Remove obsolete media commands, children, timestamp filters and their native
  lifecycle-test executable dependency. No selectable legacy media fallback or
  per-consumer RTSP reader remains. JPEG snapshot scaling still needs the executable.

**Exit:** one coherent production path covers live video, live audio and recording.
The production wiring/removal exit is met. Deployment, Apple HSV acceptance, full
privacy and sustained recovery/resource qualification remain separate requirements.

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
  - [x] Record user observation: Home playback works, pitch sounds right and burned-in
    seconds advance approximately correctly, but audio is silent roughly 3/5 of the time.
  - [x] Correct HomeKit audio RTP/RTCP clock units without changing canonical PCM timing;
    test all negotiated-rate/packet-time combinations, offsets, wrap and sender reports.
  - [x] Obtain post-fix listening confirmation: the user said “that fixed the audio.”
    This is real Home audio feedback, not measured physical lip sync or a long soak.
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
  - [x] Decode two successive clips from each shared recording epoch at 32/48 kHz and
    video-only; observe one acquisition and zero released-output charges.
  - [ ] Qualify automatic controller continuation and high-pressure size/duration cuts.
- [ ] Required camera clock/codec representations are supported or explicitly refused
  with useful diagnostics; no unsupported case receives guessed timing/admission.
- [ ] Nix package/service and the documented Cargo workflow provide the Rust library
  dependencies, without project C workers or system-wide tool installation.
  - [x] Check in `.envrc` so direnv loads the pinned native build dependencies; ignore
    its local cache and document approval plus hook-free Cargo commands.
- [x] README describes the implemented shared pipeline, retained FFmpeg uses and
  separate outstanding deployment/acceptance requirements.
  Remove obsolete research entrypoints when no longer useful; retain compact findings
  and raw numeric artifacts only where they still explain a decision.

**Definition of done:** Rust-owned shared acquisition is the only production camera
reader; live/audio/HSV work on real devices with independent transport state, bounded
owned resources, privacy/recovery behavior and recorded user acceptance. The C/relay
detours and legacy direct-reader implementation are gone—not hidden behind a switch.
