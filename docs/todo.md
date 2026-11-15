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
- [ ] All project-owned C required for the application is replaced; obsolete C is removed.
- [ ] Real-device live/audio/HSV, privacy, recovery and resource acceptance pass.

## 1. Replace the C-based draft with Rust (start here)

- [ ] Choose and pin Rust bindings compatible with the FFmpeg libraries in Nix. Check
  public packet side-data/RTCP metadata, codec parameters, time bases, read interruption,
  muxing and AAC/Opus APIs against the real camera. Use audited Rust FFI where needed;
  no custom C shim, private FFmpeg structs or new relay dependency.
  - [x] Pin `ffmpeg-sys-next = 9.1.1` against Nix FFmpeg 9.0.2; compile public
    packet/RTCP SR, codec-parameter and interrupt APIs and read the real main stream.
  - [ ] Exercise Rust muxing and AAC/Opus APIs; characterize real RTCP cadence/A/V clocks.
- [x] Replace the unfinished `src/ingest.rs` worker/IPC implementation with Rust-owned
  library acquisition. Reuse sound contracts, not its standalone-C process assumptions.
- [x] Define owned packet/configuration/error types, reference lifetimes, thread-safety
  boundary and interruption/shutdown behavior before exposing subscriptions. Redact
  credential-bearing URLs in native-library diagnostics as well as Rust logs.
  The blocking reader is thread-bound; immutable packet/configuration owners cross
  threads. Deadlines and sticky cancellation interrupt library I/O; Drop closes it.
  `src/source/` now adds joined supervision and bounded subscriptions. Production
  injection and copied-output/transport privacy are still section 2/3 work.
- [ ] Complete every applicable row below; update Cargo/Nix and remove stale entrypoints.

| Project C / related scaffolding | Required Rust replacement or removal |
|---|---|
| `native/ingest.c` | **Removed.** `src/ingest.rs` owns libavformat open/read/interrupt/close, codec metadata and packet/side-data lifetimes; no production consumer is connected yet |
| `probes/ingest_worker.c` | Production hook removed; historical fixture exporter remains until obsolete probe launchers are retired |
| `native/output.c` | **Unused C draft removed.** Rust RTP and fMP4 consumers still need implementation/integration; historical mux behavior remains in `probes/mux_worker.c` |
| `probes/mux_worker.c` | Carry required codec-parameter reconstruction, packet-copy mux and validation behavior into Rust; retire the historical C sink |
| `probes/packet_wire.h` | Remove worker-only framing when typed Rust packets replace IPC; use a bounded Rust parser only if retained diagnostic artifacts need it |
| `nix/packages/native-media.nix` | **Removed.** Application package and dev shell now supply FFmpeg libraries, pkg-config and bindgen dependencies |
| `nix/packages/ingest-worker-probe.nix` | Retire obsolete C/probe build dependencies, including json-c where no longer used; do not add a replacement synthetic rollout gate |
| Python worker launchers / unused AMC2 scaffolding | Remove or clearly retire obsolete worker entrypoints after their findings are preserved; do not leave the application dependent on the research harness |

**Exit:** required media behavior is Rust application code using upstream libraries;
no project-owned `.c`/`.h` is needed to build or run amcrust. Removing obsolete
probe-only code is preferable to porting an unused IPC architecture just to keep it.
This does not require rewriting FFmpeg itself.

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
    non-increasing DTS retires the generation. This is a refusal policy, not A/V proof.
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
    cannot be retracted by this cooperative library layer.
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
- [ ] Live audio: use shared AAC input; own FFmpeg-library decode/Opus encode in Rust.
  Pool only compatible negotiated settings and isolate encoder/subscriber failures.
- [ ] `src/hsv/recording.rs` / `src/hsv/fmp4.rs`: remove the recording camera reader;
  consume generation-bound shared packets, mux through FFmpeg bindings and preserve
  correct initialization, prebuffer, actual-IDR start, historical/live order and cuts.
- [ ] Recording output: keep ownership through muxing and HDS delivery, including
  pending writes, timeout, cancellation, repeated cancellation and temporary storage.
  Separate accepted bytes, local finish/end-write and confirmed user-visible recording.
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
- [ ] README accurately describes the implemented pipeline and operating requirements.
  Remove obsolete research entrypoints when no longer useful; retain compact findings
  and raw numeric artifacts only where they still explain a decision.

**Definition of done:** Rust-owned shared acquisition is the only production camera
reader; live/audio/HSV work on real devices with independent transport state, bounded
owned resources, privacy/recovery behavior and recorded user acceptance. The C/relay
detours and legacy direct-reader implementation are gone—not hidden behind a switch.
