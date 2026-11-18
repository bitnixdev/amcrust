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
| Camera acquisition | Live video, audio and recording share Rust libavformat readers by encoder | One Rust-owned libavformat acquisition per active encoder |
| Live video | Shared Rust H.264 packetization, host-monotonic presentation and Rust-scheduled SRTP/SRTCP; no video child process or local video relay | Shared encoded H.264, Rust per-viewer packetization and existing encryption/control integration |
| Live audio | Shared AAC acquisition, joined Rust AAC/resample/Opus workers and independent transports | Rust-owned FFmpeg-library audio processing shared where negotiated settings match |
| Recording | Shared packet history, recovered AAC, joined clip-local Rust mux and source-fenced HDS | Shared packet subscriptions, Rust-owned mux/lifetimes, existing HomeKit/HDS integration |
| Experiments | Retained numeric findings; obsolete C/Python fixture harness removed | Behavioral evidence to carry forward, not a second production implementation |
| Rust acquisition boundary | `src/main.rs` injects the same main source into live video/audio and HSV; joined supervisors and bounded subscriptions own libavformat | Qualify sustained operation and integrate aggregate metrics |

**All production media paths have switched to shared Rust ownership.**
Real checks exercise this wiring, but are not deployment or HSV Apple acceptance.
The user confirmed Home live playback and that the audio wire-clock correction fixed
its gaps. C workers, per-viewer FFmpeg commands, the recording reader/mux command,
relays and timestamp filters are removed. No selectable legacy media fallback remains.
JPEG snapshot scaling still uses an executable, not a second RTSP acquisition.

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
              └─ Recording → recovered AAC + encoded video history → clip-local cut
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
  codec/resampler state. `media::recording` normalizes recording media without opening
  a reader; `hsv::fmp4` owns history, clip admission and joined application workers.
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

## Production timing and recovery decision

The user selected **stable playback with an explicit recovered presentation clock**,
not source-derived capture-time fidelity. Cameras are underpowered, untrusted long-term
clock sources; production must recover unattended across months of operation, camera
reboots, network outages and other failures. This is the implementation direction,
not a claim that unattended in-session recovery or Apple recording presentation is qualified.
Production video now uses host-monotonic packet-read times; that initial presentation
model still needs sustained, network-burst and paired-client A/V qualification.

Preserve original PTS/DTS, SR/PRFT and contradiction diagnostics separately from
presentation timing. Implement a common monotonic A/V presentation domain with bounded
continuous audio resampling; qualify buffering, onset, drift, stalls and recording
cuts against actual playback. Host arrival is not capture time, and independently
zeroing tracks is not synchronization. Do not silently pad, discard media, reanchor,
or widen the old candidate's budgets to hide its failure. The source-anchored candidate
below remains historical/diagnostic; making this camera pass it is no longer a
production gate. Its alternatives table records the earlier choice, not this decision.

Recovery must be demand-driven and bounded, without a permanent circuit-breaker latch.
Old source grants stay revoked. Retry does not resurrect stale decoder/recording state,
complete a failed clip, or permit resetting SRTP counters under reused keys/SSRCs.
Fresh-session recovery and any future in-session recovery need explicit cryptographic
and media-epoch ownership. Privacy disable and shutdown override every retry.

## Implemented source-library boundary

`source::Registry` registers camera/encoder identities once, retaining credentials in
its supervisors. Consumer handles cannot retrieve URLs. An idle source has no reader;
the first subscriber starts one blocking libavformat job, shared across requested
tracks. Failure retires all old leases. New subscriptions observe bounded backoff and
a consecutive-failure circuit breaker; they never resume inside an old generation.
After a failed burst, the breaker cools down (30 seconds by default, configurable
between the retry cap and 300 seconds) and permits fresh demand without operator reset.
Cooldown expiry does not open an idle/disabled source or revive an old subscription.
Explicit `Registry::shutdown` signals all owners before joining and can be awaited
again if cancelled. Call it before dropping the Tokio runtime; Drop signals cleanup
but cannot provide an async join by itself.

Production creates its registry with `Timing::Presentation`; the library default
`Timing::Source` remains for source-clock diagnostics and the existing finite/audio
experiments. A generation has one immutable timing mode. Presentation admission checks
packet identity, corruption, current parameter sets and slice references; it refuses
B slices rather than guessing reordered timestamps. Missing/rolling-back camera clocks
remain recorded, not overwritten, and do not alone retire a presentation generation.
Changed report identity, codec changes and I/O failures still retire it. Queue span
uses the same monotonic domain while count/byte/residence budgets remain unchanged.
Strict audio/mux constructors reject presentation deliveries instead of accidentally
using their unreliable camera timestamps. The fixed-anchor audio candidate is not a
presentation-mode fallback. The source-clock behavior described below remains diagnostic.

Current source-clock admission supports Annex-B H.264 with one matching SPS/PPS pair and checked
slice references. It waits for a future actual timestamped IDR, never a cached GOP or
key flag alone. Initial unclocked packets are skipped, not repaired; later clock loss
or unexplained non-increasing DTS retires the generation. A narrowly classified AAC
SR-mapping step instead retires strict audio/AudioVideo subscriptions locally; explicit
`Tracks::AudioSampleClock` subscriptions can continue with unchanged packet timestamps.
Video-only subscriptions are unaffected by that audio-local event. Irregular steps are
preserved. These decisions do not establish source A/V alignment or decoded presentation.
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
before that packet is published. The generation snapshot retains one first-failure
record: fault kind, stream/sequence, optional original PTS/DTS, previous DTS, duration,
flags, byte count and public SR/PRFT metadata. Admission's later missing timestamps
and non-increasing DTS now receive explicit fault kinds too. Metadata refusals retain
the previous observed DTS; admission refusals retain the previous admitted DTS.
No packet body, URL or credential is retained by this record; it survives retirement
and is reset only for a new generation. Recording a reason does not change admission.
Modular wrapping is handled within the half-range
constraint; duplicates do not refresh report age. Retirement is explicit and a new
reader gets fresh observations. No report-age deadline, drift tolerance or affine
clock correction is inferred from the short device samples. The observed AAC clock
contradictions, including larger SR-boundary jumps, remain unresolved for live A/V.

### Bounded AAC sender-report remapping

`src/source/audio_timing.rs` uses only public SR/PRFT fields, with the exact integer
semantics audited in pinned FFmpeg **9.0.2** (`rtpdec.c:rtp_set_prft/finalize_packet`,
`utils.c:ff_parse_ntp_time`, and the MPEG4-GENERIC AAC depacketizer/header parser).
For 48-kHz AAC-LC, invert PRFT minus the cached report's NTP microseconds to obtain
an RTP timestamp **reconstructed from metadata**, not independently captured on wire.
The inverse must round-trip exactly, with flags 24 and a signed delta within 15 s;
unsupported NTP eras/provenance and missing metadata provide no exception. Preserve
first/last rollback evidence and bounded counters, never packet bodies.

A source exception requires consecutive usable AAC observations, 1024-tick durations,
PTS equal to DTS, no corrupt flag, unchanged report SSRC, advancing report NTP/RTP,
and fresh distinct report evidence under 15 s. Reconstructed media RTP must advance
by 1..4800 ticks despite mapped DTS going backward/equal. The difference must match
the SR mapping change to within the **one-tick integer-rescaling ambiguity**, not a
measured jitter allowance. No guessed timestamp, dropped AU, padding or reanchor occurs.

The mapping-step cap is **9600 ticks / 200 ms**, the diameter of the existing
**±4800-tick / ±100-ms** phase window: e.g. +95 ms to −57 ms is a 152-ms step but both
endpoints fit. This is not a larger phase budget. The consuming anchored-sample
processor still checks every decoded endpoint against its original anchor and the
unchanged 100-ms/15-s limits; the source exception does not grant a transport clock.
Only the explicit audio-only subscription opts in. Strict audio and AudioVideo/mux
subscriptions are revoked before publication of that packet, including held output
grants, while unrelated video stays live. Missing clocks and bad metadata retain
generation retirement on active tracks. Non-increasing DTS with reconstructed RTP
rollback/duplicates or unclassified evidence still retires the generation. An explained
but oversized AAC mapping change instead quarantines audio as described below; it
never receives permission to continue audio. Initial-clock/IDR and queue budgets are
unchanged.

### Audio quarantine for oversized explained changes

An explained AAC-LC/48-kHz mapping change outside the 200-ms step cap invalidates
**all** audio-bearing subscriptions, including `AudioSampleClock`, without retiring
healthy video. The offending packet is not published. This is a failure-containment
policy, not a larger timing allowance: the sample-clock phase/freshness limits and
bounded continuation classifier are unchanged.

Quarantine is sticky for the entire source generation. New Audio, AudioSampleClock
and AudioVideo requests fail with Clock while that generation remains active; there
is no silent audio restart or new anchor. Video-only subscribers can continue/join.
With no remaining demand the reader idles out normally; an explicit owner reset or
subsequent fresh acquisition creates new observations and clears quarantine. Existing
failed grants never revive. `TrackSnapshot.quarantine` retains the first offending
metadata separately from whole-generation `fault`/`failure`.

Quarantined AAC timing observations freeze and later AAC packets are discarded, but
their generation/index/global sequence identity is still checked. Video timing and
whole-reader failures, configuration changes, cancellation and shutdown remain fully
guarded. Malformed metadata rejected by acquisition is not swallowed. This keeps an
unavailable audio track from repeatedly poisoning a healthy shared reader; it does
not provide clock recovery, repair recording cuts or fence already copied output.

Real checks did not yet observe this new quarantine path: one encountered a separate
video DTS rollback and another saw no oversized audio event within 300 seconds.
The existing smaller, bounded audio exception has prior live evidence. Do not claim
oversized audio recovery or protection from video's own clock faults from that result.

This handles a narrow, explainable discontinuity; it is not loss detection, capture
clock qualification, an adaptive resampler or a recording edit policy. The real camera
also exceeded the candidate's cumulative phase budget independently of rollback.
Before quarantine was added, a real window exercised the smaller bounded exception:
strict audio stopped while candidate audio/video continued until a later 276-ms shift
retired the generation.
This establishes isolation, not successful sustained qualification.

`audio_progress` separates elapsed reconstructed RTP, mapped PTS and declared AAC
sample duration over one explicitly identified prefix. It records exact duration
debts, total mapping change, the sum of rounded SR-predicted mapping changes and the
remaining unexplained residual. No floating-point fitting participates in this
accounting or in admission. Missing/unusable evidence freezes the prefix with a
sequence marker; it never silently reanchors. Measurable mapping residuals are retained,
not repaired or used to relax the separate one-tick rollback classifier. Counter
rounding is not capture-clock evidence. Source observations can run ahead of decoded
input, so compare endpoint sequences/counts and anchors before equating their phases.
A real matched endpoint had 1456 ticks of reconstructed RTP duration debt, 144 ticks
of SR shift and 16 unexplained ticks: all three sum to the 1616-tick processor phase.
Removing SR updates alone therefore does not solve that window's discrepancy. This
does not distinguish oscillator error, producer timestamp jitter or missing media;
clock recovery still requires explicit policy and physical A/V qualification.

Queue and held-delivery charges persist through retirement until owners drop. Source
and registry budgets include bounded reader handoff and descriptor charges. Access
checks and lease retirement are cooperative: already borrowed/copied bytes cannot be
retracted. Source-bound transport writes now use the atomic retirement fence below;
production registry injection, recording history/copies, aggregate output-job limits
and Prometheus wiring remain integration work. Logical charges are not a bound
on FFmpeg internal allocations, allocator overhead or process RSS.

## Implemented output-library boundary

`media::rtp::H264` packetizes a charged source delivery into bounded caller-provided
storage (single NAL or FU-A). Each viewer owns its sequence and RTP clock origin plus
negotiated SSRC/payload type/plaintext packet limit. SPS/PPS accompany each IDR; the
last packet alone carries the AU marker. Abandoning a partial AU fails that packetizer.
The shared secure transport below owns production socket writes. This
packetizer is now connected to HomeKit sessions with an explicit presentation clock;
`Transport::rtp_capacity()` reserves the SRTP overhead from the wire limit.
`H264::new_presented` uses immutable packet read instants in a host-monotonic domain.
`sender_clock` samples that same RTP clock against host UTC for current SRs, not the
last packet or camera NTP. All AUs are passed through without timestamp clamping or
frame dropping; this does not guarantee jitter smoothing or physical lip sync.

`media::fmp4::Muxer` uses public FFmpeg parameter-copy, packet-reference and custom AVIO
APIs for finite H.264/AAC or video-only output. Its first video input must be the
source-issued IDR used to create it. The strict diagnostic constructor retains exact
source-PTS translation and its historical sample-duration discrepancy evidence.
Production uses `new_presented`, normalized video stamps and recovered AAC from the
same actual subscription, not merely an equal generation number. Encoder parameters
are copied from the AAC encoder and cannot be mixed with another codec job. The
video IDR is the common origin; converting that origin to the AAC sample grid rounds
down by less than one sample. Tracks are never independently zeroed.

Mux state is thread-bound and should run on a blocking media worker. A shared pool
reserves finite input/output and chunk allowances before admission. Copied chunks keep
the job reservation and source grant, rechecking cancellation, lifetime and generation
before access. Abandonment is not finish; explicit finish is only local mux completion.
`WriteGuard` carries source/subscription, cancellation, job failure and lifetime
permission into HDS's final syscall gates. Already accepted kernel bytes cannot be
retracted. Reservations are logical ownership bounds, not FFmpeg allocator/RSS measurements.

## Production shared recording

The recorder receives the main `Source`, never an RTSP URL. One joined blocking
processor consumes an AudioVideo subscription (Video when recording audio is off).
Video stays encoded. Before video admission, a counted, at-most-15-second warmup uses
the lower arrival envelope and declared frame cadence; a later actual IDR chooses the
sole anchor after at least one second. Thereafter the same bounded recovery controller
used by audio adjusts video durations, at ±5000 ppm, without losing/repeating an AU or
reanchoring. This assumes the configured encoded cadence; large cadence changes,
reordered pictures and unsupported phase/gaps fail the epoch rather than guessing.

A single continuous AAC worker per recording epoch reuses the Rust decoder/resampler
core and emits mono 32/48-kHz AAC-LC, with private encoder ordinals in a common host
presentation domain. This is lossy re-encoding, not AAC packet-copy. Original camera
packets and timing diagnostics are unchanged. Its planar-float output, AAC parameters
and 1024-sample units are distinct from Opus wire semantics. The production camera
configuration keeps acquisition AAC at 48 kHz; changing recording output rate alone
does not reconfigure the source or interrupt live audio.

History owns charged video deliveries and bounded encoded-AAC outputs. Snapshot and
live subscription are atomic with publication. A clip chooses the latest actual IDR
at or before the requested prebuffer start, so the window may extend backward by a
GOP. AAC starts at least two complete AUs before that IDR. Each clip gets new
initialization and relative timing, not a reused persistent-mux init. Negative AAC
preroll and MP4 front edits remain explicit; decoder-state/physical onset equivalence
on Apple devices is not inferred from successful FFmpeg decoding.

Finishing freezes the video selection and waits up to two seconds for covering AAC.
The last whole AAC AU may extend past video by at most one AU plus the sub-sample
origin quantization; there is no silence insertion, partial-AU trim or fabricated
video hold. Already overextended audio, lost input, missing coverage or mux failure
refuses completion. HDS retains one fragment and labels it final only after explicit
successful mux completion. Streams request a clean cut after the eight-second
post-motion tail, 60 seconds of delivery, or 48 MiB of selected input. Subsequent
controller requests select overlapping history. Automatic Apple continuation is
unqualified; an accessory-generated EndSent is not an iCloud acknowledgment.

Bounds are separate and concrete:
- History: 12 seconds, 12 MiB and 1536 entries; the source subscription also caps
  1024 charged packets, 12 MiB, and 20-second residence/span. Fresh processing and
  history admission are checked separately from retained-history residence.
- AAC: one 12-MiB logical job reservation, at most 1024 eight-KiB output packets,
  and 16-second output residence. This does not measure native RSS.
- Mux: one admitted clip with a 140-MiB logical pool; 64 MiB each of finite input
  and output, 16384 packets/chunks, and a 120-second hard lifetime. Clean cuts aim
  to finish before those hard refusal limits.
- Live history broadcast: 128 frames; clip output: two messages with two-second
  input/output-pressure deadlines. MP4 boxes/fragments cap at 16 MiB, init at 1 MiB.
- HDS retains its sixteen prepared-session/eight-connection bounds and five-second
  frame-write deadline. Partial ciphertext poisons the connection.

Configuration, recording epoch, source/mux and stream gates compose at the final
nonblocking HDS write. Encryption, copying and readiness waits stay outside them.
Source retirement also invalidates retained init/media bytes. Stop revokes first,
then joins the history processor and all admitted mux workers. Join handles remain
owned if an awaiting stop is cancelled. Drop only signals cancellation. The recording
busy slot belongs to the delivery owner, not stale ACK/close callbacks.

After failure, the history supervisor clears the failed epoch and retries fresh
subscriptions with a bounded 1..30-second backoff. It never revives old output grants;
fresh history and a new controller stream are required. Recording disable leaves live
subscriptions valid. Reconciliation reads current desired state under its ownership
lock before acknowledging changes. Whole-camera live privacy transitions, automatic
controller outage recovery, allocator-wide accounting, and Apple recording acceptance
remain separate open work.

## Production resource telemetry

The metrics server samples the actual StreamManager and Recorder, not diagnostic-only
copies. Source snapshots expose fixed main/sub_stream2 roles, registration/activity,
generation, open/fault counters, subscriber counts, logical charges/limits and sampled
oldest active-subscription delivery age. Unregistered substream roles are marked absent,
not given fabricated source counters. Observation never subscribes or opens acquisition.

Live Opus, recording AAC and recording mux pools expose reserved jobs/bytes and limits.
Reserved slots include ownership retained by output objects; they are not thread or
active-codec counts. Live owner counts include prepared sessions and retained video/audio
owners, including teardown. Recording history reports payload bytes, entries, end-time
span and retained clip owners. These views overlap: source charges, history payloads and
full finite-job reservations cannot be summed as memory use or compared directly to RSS.

Owner/history sampling uses try-locks. If contended, health returns null for that view,
Prometheus emits an availability gauge of zero and omits its dependent counts. The
existing recording-pipeline gauge is NaN rather than false when its state is unavailable.
Source/pool fields are independently sampled; the result is not an atomic global ledger.
The old awaited recorder-state lookup is removed from HTTP scrapes.

Health's status is explicitly process liveness and its backend identifier describes the
shared production implementation. An active generation or pipeline does not establish
decoded playback, capture synchronization or camera readiness. Allocator/native RSS,
complete aggregate admission, richer recovery-state metrics and matched real workload
evaluation remain separate work.

## Encoder configuration transactions

Recording selection now coordinates configuration, camera mode and recorder ownership.
Before an encoder-affecting write it marks configuration uncertain, retires snapshot
epochs, closes main-source admission, revokes/joins affected live owners and stops
recording. Prepared live sessions are discarded; an already running independent
substream video owner is not stopped by this application. Firmware may still reset
other streams when Encode settings change; that behavior needs real qualification.

The existing camera settings/readback helpers run only after retirement. Their errors
are propagated instead of logged-and-continued. Success resets the main supervisor's
retry state, publishes the new live profile and reopens main demand only if privacy and
shutdown allow it. Old subscriptions, transports and snapshot copies never revive.
A failed or cancelled encoder transition leaves a sticky uncertain state; camera-on or
a snapshot switch cannot bypass it. Retrying even the last accepted selection performs
repair rather than being ignored as a duplicate. External camera writes are not atomic:
there is no fabricated success or automatic rollback after a partial camera failure.

Recording-output-only AAC changes restart recording without mutating the encoder,
retiring live acquisition or invalidating snapshots. Incomplete recording reconciliation
is tracked separately from uncertain camera configuration so a retry does not needlessly
reconfigure live media. Persisted settings are forcibly reconciled before startup exposure;
a startup settings failure is now an error, not best-effort continuation.

Live configuration reads reflect current profiles. Selection matches dimensions and
frame rate, refuses absent/stale formats and rechecks the main profile under admission
ownership to close selection/reconfiguration races. The old arbitrary first-source
fallback is removed. Failed starts and unsupported resume/reconfigure commands now
surface errors to HAP; fresh setup is required rather than reusing retired session state.
This does not establish that Apple clients refresh cached capabilities or recover
automatically after a source change.

## Camera-mode lifetime

RTP Stream Management Active is a separate, persisted live-only gate. This application
has one such service, so the gate covers all of its live sessions, not one viewer.
Inactive closes setup/start admission, discards prepared sessions and revokes/joins
live video/audio without disabling shared sources or recording/snapshot permission.
Enable completes any interrupted disable before reopening and requires fresh setup.
Duplicate enable does not terminate a healthy live session. Effective admission also
requires Camera Active, a current profile and a non-shutdown manager.

Both persisted gates are applied before HAP exposure. Existing valid state files
without the new live flag retain their other flags and default live intent to enabled,
matching the previous behavior. Only a missing file receives first-launch defaults;
unreadable/malformed state disables media modes. Persistence is serialized and uses
same-directory temporary-file replacement instead of truncating the live state file.
Write failures remain logged/best-effort; power-loss durability is not established.


HomeKit Camera Active now reconciles live admission, all encoder sources and recording
under serialized mode ownership. Off revokes live controls and clears prepared sessions,
disables source owners (invalidating escaped recording/RTP grants), then joins live and
recording work before acknowledging. On allows fresh demand/setup; no old session or
grant is revived. Startup applies persisted privacy before exposing HAP consumers.
Shutdown permanently closes live admission, including against concurrent enable calls.

Live teardown retains owners in their maps and awaits borrowed join handles. Cancelling
a wait therefore leaves the join owned for a later teardown, rather than detaching it
via remove/drain-before-await. An interrupted mode change is not an acknowledged
completion; it can be reconciled again. Recording-only disable remains independent.

Snapshots now carry a sticky epoch through cache publication and final encrypted HTTP
writes as well. Camera and snapshot-reason changes retire the previous epoch; re-enable
creates a new one. Cached/scaled fallback images from retired epochs are never reused.
A request captures permission before reading mode flags, closing off/on and reason-change
races. The refresher has drop cancellation, cancellable HTTP fetching and an owned,
repeatably joinable shutdown handle instead of a detached forever loop.

The HAP adapter binds a permission to each snapshot response via a connection-local
identifier. Its serializer parses complete fixed-length responses across partial flushes
and pipelined responses, not merely the first JPEG-looking buffer. Registered permissions
cap at sixteen per connection; headers at 64 KiB and individual response bodies at 16 MiB.
Current HAP handlers use finite bodies; chunked/ambiguous framing is explicitly refused.
These are not aggregate connection, image-cache or native allocator budgets.

A write holds the epoch read gate only across the nonblocking socket poll/syscall;
encryption occurs outside it. Retirement wakes blocked writes. Any retired/failed partial
response poisons the connection, preserving ciphertext/counter integrity rather than
appending another response. Already kernel-accepted bytes cannot be retracted. Opt-in
local snapshot saves carry the same permission in an awaited blocking-file job. Shutdown
clears cache storage; retirement alone invalidates retained copies rather than claiming
their memory is freed.

Actual Home mode/automation behavior, real encoder-change/firmware and controller
qualification, mid-response close handling and long outage/reboot recovery remain open. This wiring
is not complete camera-wide Apple privacy acceptance.

## Implemented secure transport boundary

`media::transport::Transport` is now the **only production live SRTP/SRTCP socket-write
path**, used by the Rust video/audio owners in `src/stream.rs`. It owns the UDP socket, fixed peer, outgoing SSRC and cryptographic state.
It does not acquire media, pace packets, rescale timestamps or invent sender reports.
The legacy timestamp adapter is removed. Rust audio processing uses 48-kHz presentation
units; the HomeKit producer converts signed, unwrapped offsets and sender reports into
the negotiated HAP audio rate before adding its independent RTP origin.

Outgoing protected datagrams must fit the configured wire limit (maximum 2048 bytes), including
10 bytes for SRTP or 14 for SRTCP. Production reserves SRTP overhead before packetization,
caps requested MTUs at 1378 bytes and refuses requests below 188 rather than
sending beyond them; invalid audio MTU disables audio without stopping video.
One send and one receive operation may be active per transport; concurrent calls are
refused, not queued. A write has a 250-ms readiness budget. WouldBlock retries reuse
ciphertext, not cryptographic counters. Feedback is bounded, restricted to the peer,
authenticated and replay-protected with a 128-packet window. Only the first authenticated
feedback SSRC is retained; a different sender requires a new session. Unauthenticated
SSRCs cannot accumulate retained replay state.

`revoke()` synchronously fences the nonblocking socket write under the same gate and
wakes pending waits. Session replacement/end/suspend/shutdown revoke every affected
track before the first awaited cleanup, then joins its media owner. Production no
longer keeps separate child-process maps: a temporary native adapter attaches each
FFmpeg child to its media owner. Producer exit revokes the transport and joins the
proxy; transport termination kills/reaps the child and joins its bounded diagnostic
drain. Drop signals revocation but cannot join; explicit stop must be awaited.
Already kernel-accepted datagrams cannot be retracted.
The producer predicate is checked before encryption and immediately before writing,
but **is not an atomic source-generation fence by itself**.
`Transport::new_source_bound` binds to both the actual generation and subscription
of its first delivery. The final nonblocking syscall holds both retirement read gates;
retirement takes their write side before acknowledgement. Producer callbacks and
cryptography run outside these gates. Sticky notifications wake pending sends and
feedback, including retirement between a producer predicate and the syscall. No
per-transport callback registry or rebinding is needed. Subscription-local audio
failure leaves unrelated video transports usable; generation retirement closes both.
Already copied plaintext cannot bypass the fence, even with a stale `true` predicate.
Both Rust live owners now use this binding. Its pending-start control fences stop
before subscription/first IDR, refuses late transport installation, joins cancellation
and releases retired transport/source references even while an ended HAP entry remains.
The unbound constructor remains a library/test facility, not a production live path.
Recording uses its own source-bound mux/write guard and composed HDS leases.
Allocator-wide accounting and whole-camera privacy transitions remain TODO.

Real-source H.264 from two Rust viewers decrypts/decodes through independent FFmpeg SRTP
receivers. This is not Home app acceptance. Production video no longer uses the FFmpeg
executable or its RTP/report clock. Live audio has also replaced that adapter with
Rust-owned codec workers and presentation/report timing.
Authenticated receiver feedback is now consumed in Rust, not sent back to FFmpeg.
Receiver priming, sustained playback and Apple recording qualification remain required.

### Rust-owned receiver feedback and live lifetime

`rtcp::Receiver` consumes at most 2048 authenticated plaintext bytes from `Transport`.
Parsing is transactional: a malformed trailing block cannot partly update report
state, pin an identity or consume a FIR sequence. It retains one remote participant,
one last report for the local SSRC and saturating counters, not an SSRC map, CNAME,
reason string or packet history. SR/RR report fields preserve signed cumulative loss,
extended sequence, raw jitter/LSR/DLSR; they are not playback acknowledgement or an
RTT/clock estimate. Reports for other media SSRCs do not replace this track's state.

PLI, FIR and generic NACK shapes are validated. Consecutive repeated FIR request
sequence numbers are deduplicated, including wrap; NACK counts requested occurrences,
not unique loss. Requests are observable but do not force a camera IDR, retransmit
packets, change rate or reset clocks. No such mechanism has been implemented or is
claimed by removing the legacy feedback-forwarding socket path. Unknown framed blocks
are counted without retained payloads. SDES/BYE lengths and final-only padding are
checked; an authenticated BYE for the bound participant terminates this track.

The production media owner revokes writes and joins its Rust producer, including
native codec cleanup on the blocking audio worker. Explicit HAP stop/replacement fences all affected tracks before
awaiting any owner. Unexpected producer exit also revokes media; video transport
lifetime, not stale child-map membership, drives the active-live gauge. Fixed-label
`amcrust_receiver_feedback_total` counters distinguish accepted datagrams, report
packets, requests, unsupported blocks, BYE and semantic refusal. Authentication/replay
refusals remain at the transport boundary. Live producers no longer have child stderr.

The historical native-adapter check decrypted 100 RTP packets and an SR, sent RR,
PLI and BYE, and observed producer reaping. That adapter is now removed; subsequent
shared video/audio checks are recorded separately in Evidence, not Apple acceptance.

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

Both live producers now own host presentation-to-NTP pairing, report scheduling and
source retirement fencing. The native report extractor and timestamp rescaler are
removed. Each session gives both tracks the same monotonic clock and CNAME, with
independent RTP origins and crypto state. Apple recording and measured common A/V
playback qualification remain open. Keyframe/retransmission requests are observed, not fulfilled.
A report formatter is not a clock policy or an audio preroll/trim contract. Neither
this API nor the accepted-write counters establishes Apple playback or common A/V
capture-clock accuracy.

## Production recovered audio

The joined blocking worker in `src/stream/shared/audio.rs` consumes shared main-encoder
AAC without opening another reader. `Transcoder::new_presented` gives a private decoder
packet copy a sample ordinal; original shared PTS/DTS and diagnostics remain unchanged.
Bounded pre-admission decoding warms AAC history and excludes probe backlog until a
paced observation; skipped startup samples are counted explicitly. Admission is limited
to two seconds of time/samples. The first admitted frame chooses the sole anchor in the
session's common monotonic domain. No subsequent frame is discarded or reanchored.

A 1/16 low-pass and ten-second proportional horizon drive public
`swr_set_compensation`, capped at ±5000 ppm. Normal phase is ±100 ms; a **positive**
late-arrival excursion up to 500 ms holds the existing correction for less than 500 ms,
allowing an intact delayed burst to catch up without silence, dropping or reanchoring.
Larger, negative >100-ms, or persistent excursions fail the processor. Phase/peak and
late-observation counters remain visible. This is a new arrival-jitter policy, not a
change to the historical source-clock candidate's limits.

RTP sends complete Opus packets, including initial codec preroll. Internal presentation
and decoder units remain 48 kHz, but HomeKit wire timestamps and SR pairs use the
negotiated audio rate. This HAP boundary exception is distinct from RFC 7587's generic
48-kHz Opus RTP clock (see [go2rtc's HAP implementation](https://github.com/AlexxIT/go2rtc/blob/master/pkg/opus/homekit.go)).
For 16-kHz/20-ms audio, each packet decodes to 960 samples at 48 kHz but advances the
HomeKit RTP timestamp by 320 ticks. Convert signed offsets before wrapping; do not
divide a wrapped RTP field, zero tracks independently, change PCM, or rescale twice.
The earlier production path incorrectly sent the canonical 48-kHz clock directly;
user-reported correct pitch with long periodic silence exposed the compatibility issue.
Post-fix listening confirmation on the user's Apple client is still required. No container skip metadata is presumed to
travel over RTP; timestamping includes native codec lookahead. Live stop does not flush
or claim a finite tail. Presentation-mode finite finish/coverage APIs are refused.
Eight jobs/16 MiB of logical reservations bound application audio processing; matching
viewers still have separate codec state, not deduplicated processing. Stop/BYE/source
retirement fences sends and joins native cleanup. A stopped audio owner does not stop
healthy video. This does not establish Apple priming behavior, physical lip sync,
network-burst playback quality, or automatic in-session recovery across an outage.

## Implemented audio-processing boundary

`media::audio::Transcoder` owns AAC-LC decoding, libswresample conversion,
a bounded PCM FIFO and libopus encoding on one blocking media worker. Configuration
covers rate, channels, bitrate, frame duration, VBR, FEC and expected loss; application
mode is fixed to voice. The admission pool limits jobs and retained output, but does
not yet share/deduplicate encoders with matching source/configuration. Native channel
layout, rate and sample format changes are refused rather than reused in an old resampler.

The strict/source-candidate constructors check decoder-frame PTS independently of
private encoder sample ordinals and do not asynchronously resample. `Transcoder::new` retains strict timing:
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
acceptance. The historical source-clock contradictions are retained diagnostics,
not qualification gates for the explicit presentation path below.

### Historical source-anchored candidate and revisit guide

**Historical candidate implementation: source-anchored, bounded sample progression.** Explicitly
call `Transcoder::new_with_clock_policy(..., ClockPolicy::candidate(), ...)`; the
ordinary constructor remains strict. The candidate requires **48-kHz AAC input** and
supports **8/16/24/48-kHz Opus encoder input with 20/40/60-ms frames**. This includes
HomeKit's negotiated rates and packet times handled by the existing live path.
Other configurations are refused, not rounded. Encoder-private positions and padding
are converted exactly into the canonical Opus decoder/RFC 7587 clock's 48-kHz domain.
This library contract does not choose the HomeKit-specific wire clock described above.

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

**Sustained qualification remains failed/unestablished.** The first requested
30-minute-or-first-refusal run ended with `Source(Clock)` after 34.33 s total, before
a candidate budget trip. Its last periodic observation was 688 samples/14.33 ms.
The offending source packet was not captured by the then-existing diagnostics;
new first-failure records make subsequent failures distinguishable. A separate
60-second diagnostic passed but cannot erase that failure. Do not infer oscillator
drift, select asynchronous resampling, increase tolerances or wire a production
source clock from this result alone: first distinguish a metadata refusal from an
actual missing/non-increasing media timestamp. A user-requested retry subsequently
captured an AAC DTS rollback of 96 ticks/2 ms on an SR-bearing packet after 200.19 s
total, with one acquisition and no candidate budget trip. This identifies admission's
refusal. Subsequent adjacent public metadata showed advancing reconstructed RTP and
exact SR mapping shifts of 170 ms and 152 ms. The user confirmed the north-driveway
service was stopped before the retry; it is not the competing-reader explanation.
The bounded source exception above separates those mapping changes from media
progression, without claiming an independent capture clock. A later candidate run
hit 4816 samples / 100⅓ ms of cumulative phase and was correctly refused. See Evidence
for exact outcomes. No capture-clock repair or automatic reanchor is authorized.

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
| Retain/recover per-packet raw RTP clocks before SR-based demux adjustments | Public SR RTP values describe report instants, not each media packet; PRFT is dependent evidence. A narrow 48-kHz AAC inverse now round-trips public PRFT/SR metadata for discontinuity classification; this is not an independent wire clock or a replacement presentation timeline | If independent packet evidence shows raw RTP follows decoded samples while all large jumps come from SR remapping, prefer preserving that clock over modeling it; unexplained rollover/phase jumps would indicate a bad reconstruction |
| Continuous sample clock with a fixed source anchor (**earlier candidate, bounded**) | Avoids repeated phase jumps without changing PCM, but assumes the nominal audio sample rate remains adequate | Monotonically growing phase, repeated 100-ms trips, lip sync progressively worsening in long views, or fresh sessions repeatedly starting at different A/V offsets; do not merely raise the limit |
| Slow bounded clock recovery plus asynchronous resampling (**now selected for production development**) | Requires an explicit presentation domain, filter/rate limits and real playback qualification; it is not a capture-clock reconstruction claim | Sustained, repeatable phase slope and corroborating A/V drift favor this; audible pitch modulation, oscillating lip sync or repeated corrections after each SR would indicate chasing jitter instead |
| Host-arrival clock or unanchored packet/frame counting | Network buffering is not capture time; independent track zeroing loses A/V offset | Latency changes with network load, burst arrivals producing speed changes, or session-dependent lip sync; arrival-derived presentation needs explicit buffering/recovery qualification and must not be called capture time |
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
