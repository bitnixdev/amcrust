# Established findings and current limits

This is the consolidated record of work already done, **not a second backlog**.
[Architecture](architecture.md) states the chosen design; [TODO](todo.md) is the only
implementation sequence. Old per-milestone proof/research Markdown has been replaced
by this summary. Numeric artifacts remain linked below; implementation details remain
in the Rust code and version control, not in a second archive of planning documents.
Former experimental implementation names below are provenance, not current entrypoints.

## What is actually running

Production live video now consumes encoder-keyed Rust shared acquisition, H.264
packetization, explicit host-monotonic presentation and Rust-scheduled reports.
Both live tracks now use shared acquisition; their FFmpeg commands, local relays and
legacy timestamp rescaler are removed. Recording now consumes the same main source
through bounded history, recovered AAC and clip-local Rust mux jobs; the last recording
reader/mux command and timestamp filters are removed. This is production code wiring,
not a claim that this new recording build has been deployed or accepted by Apple.
The user has separately confirmed Home live playback and the audio clock correction. Production instrumentation added first RTP/IDR timing and recording
delivery outcomes (`end_sent`, `cancelled`, `source_unavailable`, `write_error`).
`end_sent` is a successful write, not confirmed iCloud storage. The process sampler in
`scripts/ingest_baseline.py` avoids command lines/environment values; process counts
are not RTSP session counts and summed RSS may double-count shared pages.

The unused C-worker draft (`native/`, `native-media.nix`) has been removed and
`src/ingest.rs` replaced by a Rust libavformat acquisition boundary. It is compiled
as application library code and is wired into `src/main.rs` for live video/audio and HSV.
MediaMTX was investigated but not added or launched. Apple HSV acceptance, full live
privacy transitions and unattended recovery remain separate requirements.

### Serving-time shutdown and motion lifetime

Isolated child test processes installed the production stop listeners, received real
SIGTERM and SIGINT respectively, and observed the matching stop request. This exercises
OS delivery, not systemd or a running paired accessory. In the local HDS loopback test,
a stalled unidentified connection closed, admission stayed closed and two cancelled
shutdown waits could not lose the pending listener join. A later shutdown completed
only after the retained join was released. Motion deadline checks refreshed one
sensor 999 times without accumulating timers or changing the other sensor's expiry.

The service still uses explicit process exit after application cleanup for libmdns.
Service-manager stop under real media load, interruption during startup, deployed
restart and Apple client recovery have not been qualified.

### Shared live/recording job admission

Live Opus, recording AAC and fMP4 mux constructors now reserve their full logical
allowance against both their role and a common production ceiling before allocating
native codec/mux state. The shared cap is 10 jobs/160 MiB; the current maximum
configured mix requests 158.125 MiB. This preserves the previous mix, not a promise
of ten usable streams. The shared reservation lives on the existing output-retained
job rather than on the worker thread.

Local budget tests distinguished local saturation, aggregate byte exhaustion and
aggregate slot exhaustion. Failed sibling reservations left no partial charge and
did not revoke retained output ownership. Sixteen racing roles requesting three
units each under a ten-unit ceiling admitted exactly three and counted thirteen
refusals; releasing those reservations returned both observed totals to zero.
HTTP and fixed-label rendering cover parent limits and refusal reporting.
These are reservation/ownership checks: no camera, native RSS measurement,
deployment, or controller recovery is claimed.

### Aggregate acquisition budgets and retained slots

Production health/Prometheus now read the registry's existing aggregate byte/slot
semaphores through an observation-only handle. Source snapshots expose retained slot
counts/limits separately from active subscribers, plus mirrored source/registry
slot-budget refusal counters. Existing admission limits and policy are unchanged.

Local checks establish:
- With two aggregate slots and one slot per source, local saturation refuses another
  subscription while permitting an independent source. Retiring a held subscription
  leaves its slot reserved even though the source's active subscriber count is zero.
- A third source is refused at aggregate capacity. Its partially acquired local slot
  is returned, no acquisition is opened for it, and the second source's stalled native
  owner remains active and unretired.
- Dropping the retired subscription restores admission. Shutdown retires all remaining
  owners but their retained subscriptions stay charged until released; the observer
  remains usable after registry/source destruction without retaining workers.
- Direct charged-permit checks exercise both local and aggregate byte exhaustion,
  rollback of a partial charge, and a charge retained across registry drop. This tests
  logical ownership, not native packet allocation or measured memory.
- HTTP schema and fixed-label exposition include aggregate budgets and refusal counters.

No camera access, deployment, Apple acceptance, native RSS measurement or workload
savings claim is made. These acquisition-budget views overlap per-source/history/pool
accounting and must not be summed as physical memory.

Validation: 52 library +76 application +16 HAP tests, workspace integration tests
(including local budget pressure), three HAP doctests, all-target compilation, strict
library Clippy, formatting and two sampler tests passed. The bounded review returned
no findings and the pinned Nix package build passed. The existing metrics fetch_update
warning is unchanged.

### Source recovery-state telemetry

Health and Prometheus now expose bounded supervisor phases, consecutive failures,
remaining monotonic retry delay, last fault category and cooldown entry/reset counters.
The record is observational: no retry timing, admission or generation policy changed.

Local native tests cover real loopback RTSP connection resets and a stalled open:
- Three exhausted retry bursts publish cooldowns and subsequent timed resets without
  opening idle sources or reviving the original subscriptions.
- Cooling down while disabled leaves the source disabled and does not open acquisition.
- Opening differs from backoff; remaining delay declines without new demand.
- Administrative reset while disabled clears the failure/delay barrier, retains fault
  history and does not claim a timed reset. Enabling idle state does not open a reader.
- Fresh demand opens a new attempt; privacy cancellation joins the stalled native reader
  and is not counted as another source fault. Shutdown publishes a terminal phase.
- HTTP schema and fixed-label exposition distinguish initial no-fault/idle observations.

The acquiring phase is metadata acceptance, not decoded/playback readiness. Cooldown reset
counts are not successful media or controller recovery counts. No camera access,
deployment, Apple acceptance, unattended outage soak or native RSS claim is made.

Validation: 51 library +76 application +16 HAP tests, workspace integration tests
(including the new local recovery-state test), three HAP doctests, all-target compilation,
strict library Clippy, formatting and two sampler tests passed. The bounded review
returned no findings and the pinned Nix package build passed. The existing metrics
fetch_update warning is unchanged.

### Bounded live preparation and session admission

Local tests exercise production control paths using loopback UDP reservations and
controlled task owners, without opening a camera acquisition:
- 32 concurrent setup writes retain exactly 16 preparations and report 16 capacity
  refusals. Replacing an existing preparation does not require a new slot.
- Advancing preparation timestamps past the 30-second deadline rejects Start both
  with and without a preceding read; stale success/key responses are cleared and
  expired UDP ports can be rebound. Fresh preparation remains possible.
- Eight retained video session IDs plus an audio owner for an existing ID count as
  eight, not nine. New setup/start is refused without revoking existing transports.
  Same-ID setup remains possible. Finishing video alone does not release a session
  still owned by audio; completing and joining both restores admission.
- Oversized setup metadata and non-16-byte IDs retain no endpoints. Source open
  attempts remain zero throughout these admission checks.
- Health and Prometheus cover limits, retained IDs and refusal/expiration counters.

Preparations use lazy cleanup on control operations, not timed eviction of idle sockets.
The eight-session ceiling does not override tighter source/codec budgets or establish
eight-viewer playback support. No camera access, deployment, Apple acceptance, native RSS
measurement or outage qualification is claimed.

Validation: 51 library +76 application +16 HAP tests, workspace integration checks and
three HAP doctests, all-target compilation, strict library Clippy, formatting and two
sampler tests passed. The bounded review returned no findings and the pinned Nix package
build passed. The existing metrics fetch_update warning is unchanged.

### Shared-source and media-resource telemetry

Production /metrics and /health now read the actual shared-source and media owners.
They expose source generations/open attempts/faults/subscribers/charges/limits/age,
three fixed-role media pools, retained live owners and recording history. Health names
the shared backend and explicitly identifies its status as process liveness.

Local checks verify:
- Pool snapshots follow held job/byte permits across owner clones and return to zero
  after release. These checks exercise reservation accounting, not codec allocation/RSS.
- Source, pool, owner and history values format with fixed labels and units; absent
  sources are distinguished from registered idle sources.
- Contended owner/history locks report unavailable without awaiting teardown or
  substituting zero counts. The pipeline gauge becomes NaN when unavailable.
- HTTP routes expose the health schema and Prometheus values without credentials/URLs
  or opening acquisition. Existing cancelled-teardown checks observe retained owners
  before their joins complete.

Validation passed: **51 library +73 application +16 HAP tests**, workspace integration
checks and three HAP doctests, all-target compilation, strict application-library Clippy,
formatting, two sampler tests and the pinned Nix package. The bounded review returned
no findings. The existing metrics `fetch_update` warning remains unchanged.

The old awaited recorder-state scrape is removed. Samples are not globally atomic,
and source charges/history bytes/full-job reservations overlap. No new camera access,
deployment, recovery soak, RSS measurement or matched-workload savings claim is made.

### RTP-service Active and independent live shutdown

The previously hard-coded RTP Stream Management Active value is now backed by a
persisted live-only gate. Its characteristic callback validates 0/1, joins live teardown,
and reports desired state on reads. Setup/start and streaming status honor both this
gate and camera-wide privacy. Re-enable cannot revive old sessions, bypass camera-off,
or reopen shutdown; it first completes retained joins from an interrupted disable.

Local results:
- The actual HAP characteristic callback toggles admission, rejects an invalid value,
  preserves snapshot permission and refuses enable after shutdown.
- With a stalled local RTSP endpoint, live video/audio owners and prepared sessions are
  removed while the recording owner and an independent source subscription remain
  current. No second acquisition opens. This is ownership evidence, not recording
  playback or continued decoded media from a real camera.
- An interrupted live disable retains its join. An enable attempt stays blocked until
  that worker exits, then admits fresh demand only.
- Camera-off/on preserves the independent live-disabled intent; live enable cannot
  override camera-off. Old setup/session identifiers remain unusable.
- Older valid state without the new flag preserves recording/audio/camera privacy
  values. New live state round-trips. Malformed state fails closed rather than
  silently applying enabled first-launch defaults.

Validation passed: **49 library +71 application +16 HAP tests**, workspace integration
checks and three HAP doctests, all-target compilation, strict application-library Clippy,
formatting, two sampler tests and the pinned Nix package. The bounded review returned
no findings. The existing metrics `fetch_update` warning remains unchanged.

State writes now serialize current-state snapshots and atomically replace the file
instead of truncating it in place. Write errors remain best-effort/logged, and these
tests do not establish power-loss durability. No camera was accessed or reconfigured.
Actual Apple modes, automation behavior, deployment and outage recovery remain open.

### Encoder-change retirement and current live selection

The selected-recording and startup paths now use a shared transaction rather than
stopping only recording and logging camera-setting failures. Main acquisition and
affected live owners retire before the settings operation; snapshots and recording
stay unavailable on an uncertain outcome. Success publishes a current live profile and
fresh retry state. Recording-output-only AAC changes remain independent of acquisition.

Local evidence, with no real camera access or settings changes:
- Injected settings failure observes the main source already disabled, old snapshot
  permission retired and recorder stopped before the operation. The last accepted
  selection remains unchanged; camera-off/on cannot reopen the uncertain source.
  Retrying that same accepted selection executes repair and restores availability.
- Output-only AAC rate/bitrate changes, including an incomplete reconciliation retry,
  do not invoke camera settings, invalidate snapshots or open acquisition.
- Cancellation during settings work leaves main admission blocked. Shutdown during
  the operation prevents completion from reopening it.
- Successful completion updates live configuration and does not revive old snapshots.
- Two stalled local RTSP endpoints exercise actual joined live owners: main video and
  all main audio stop, the independent video control remains current, and a fresh main
  demand opens a second acquisition while the other endpoint remains on its first.
  A pre-transition profile cannot start against the changed main profile.
- Selection checks refuse unavailable/partial dimensions and unsupported frame rates,
  and distinguish two sources with equal dimensions but different rates.

Removed behavior includes settings-error swallowing, duplicate suppression that prevented
repair, the production fixed-1080 special case, wrong-resolution fallback, and silently
acknowledged unsupported stream resume/reconfigure. Current profile reads are dynamic;
actual Apple capability-cache refresh and recovery remain unqualified.

Validation passed: **49 library +67 application +16 HAP tests**, workspace integration
checks and three HAP doctests, all-target compilation, strict application-library Clippy,
formatting, two sampler tests and the pinned Nix package. The bounded review returned
no findings. The existing metrics `fetch_update` warning remains unchanged.

External camera writes are not atomic. This increment does not prove physical encoder
readback, Sub Stream 2 survival on either firmware/model, Apple playback after a change,
or unattended recovery. A real mutation-bearing qualification requires separate approval.

### Snapshot epochs through encrypted HTTP delivery

The snapshot path no longer relies on before/after booleans alone. Requests capture
sticky epoch permission before reading mode flags. Camera-off/on and periodic/event
snapshot changes retire old frames, scaled fallback images and copied response data.
Only a new epoch can publish new frames. The detached forever refresher was replaced
with drop cancellation and retained, joined shutdown ownership; an in-flight HTTP fetch
is cancelled on epoch retirement or shutdown.

The HAP serializer now distinguishes HTTP message boundaries from flush boundaries:
partial headers/bodies remain associated with their permission, and pipelined responses
are split. Current finite HAP bodies are supported; ambiguous/chunked framing is refused.
A bounded connection-local permission identifier survives serialization to the encrypted
writer. Revocation serializes with the final socket write and wakes an unread-peer wait.
A retired partial response poisons/closes the connection; previously accepted bytes
cannot be recalled. Opt-in local image saves use the same permission.

Local checks exercised:
- Existing encrypted HTTP snapshot round-trip/authentication and delivery accounting.
- Partial flushes plus pipelined snapshot/non-snapshot responses, with independent
  permissions and refusal of unfenced image responses.
- An unread encrypted TCP peer with constrained socket buffers: a pending one-MiB
  response wakes on revoke, refuses continuation and cannot append another plaintext.
- Off/on with populated raw/scaled caches: old fallback images remain retired, and
  a fresh local HTTP response supplies a new source frame.
- Joined shutdown during a stalled local HTTP fetch, no late cache publication,
  periodic/event flag changes and refusal to revive epochs after shutdown.
- Last cache-owner drop retires escaped permissions while a remaining owner preserves
  them; drop signals cancellation, whereas explicit shutdown additionally joins.

Validation passed: **49 library +62 application +16 HAP tests**, workspace integration
checks and three HAP doctests, all-target compilation, strict application-library Clippy,
formatting, two sampler tests and the pinned Nix package. Packaging now includes workspace
checks so the HAP transport tests are not omitted. The bounded review returned no findings;
a subsequent last-owner drop hardening passed the repeated workspace checks/package build.
The existing metrics `fetch_update` deprecation warning remains.

No real camera was accessed, encoder changed or recording retained for this increment.
These are local lifetime, framing and kernel-write checks—not deployed Apple snapshot
acceptance or evidence that a Home client recovers automatically from mid-image close.
JPEG scaling still uses the FFmpeg executable; removing media readers did not remove it.

### Camera-off integration and retained live join ownership

HomeKit Camera Active now gates setup/start, clears prepared sessions and disables all
of this application's encoder sources, then joins live and recording work before
acknowledgment. Startup applies persisted privacy before HAP exposure. Re-enable permits
new demand/setup only; shutdown permanently refuses it. The old live teardown's
remove/drain-before-await pattern is gone: maps retain join handles across cancelled
waits. Snapshot request checks now honor camera-off, but are not final HTTP write fences.

The user confirmed north-driveway was idle for one serial, read-only check. No encoder
settings were changed and no recordings retained. The **17.26-s** check ran two cycles:
- Generation **1**: **179 authenticated video /25 audio RTP**, then a **751,341-byte**
  recording fragment. Camera-off joined the source/live/recording owners and refused
  that retained fragment's write permission.
- Generation **2**, after fresh enable/setup: **100 video /79 audio RTP**, then a
  **682,369-byte** recording fragment with the same retirement outcome.
- Each cycle used one acquisition, left no active source, released the Opus pool, and
  reached zero source charges after retained fragment/clip permissions were dropped.
  The test authenticates RTP; it does not decode or establish Apple playback.

Local tests cover two encoder owners with stalled acquisition, simultaneous live A/V
startup plus recording, prepared-session rejection, persisted mode, old-session replay,
fresh acquisition after re-enable, permanent shutdown admission and cancelled join
ownership. Already accepted kernel bytes remain unretractable. A cancelled mode change
is not an acknowledged completion; callers may repeat reconciliation.

Validation passed: **49 library +58 application tests**, focused recovery/phase-trend
checks, all-target compilation, strict library Clippy, formatting, two sampler tests
and the pinned Nix package. The bounded review returned no findings. The existing
metrics `fetch_update` deprecation warning is unchanged.

At that stage snapshot-cache epochs/final HTTP writes were still open; the later
snapshot increment above wires them. Per-stream Active, encoder-change source
retirement/profile refresh, Apple camera-mode/HSV behavior and unattended recovery
remain open. These results do not complete whole-camera privacy
or a deployed Home acceptance run.

### Shared recording cutover and removal of the last media reader

The user explicitly confirmed the camera was idle for serial, read-only checks.
No encoder settings were changed, no captures were retained, and no old/new reader
was shipped side by side. `RecorderConfig` no longer carries a URL. Main video, live
Opus and recording receive the same encoder-keyed Rust source.

Recording now has a joined source/history processor and separately joined finite
mux jobs. Video is packet-copied with bounded recovered cadence. AAC is decoded,
continuously resampled and **re-encoded**, not packet-copied. The 32/48-kHz output
choice is separate from the production camera's fixed 48-kHz acquisition setting.
A later IDR follows counted video startup calibration; audio uses the existing
bounded startup/recovery core. There is no post-admission AU dropping, silence
insertion, reanchoring, source-timestamp mutation or widened historical clock budget.

Each clip selects an actual IDR covering the requested prebuffer, at least two AAC
preroll AUs and a fresh init segment. It waits for covering AAC before explicit mux
finish; whole-AU excess plus sub-sample origin quantization is reported as such, not
a sample-exact end trim. Retained output keeps configuration/source/mux permission
through encryption to the final HDS syscall. Stop joins history and admitted clip
workers, including when a prior join wait was cancelled. Failed epochs retry fresh
subscriptions; they do not revive an old HDS stream or crypto/session owner.

Real outcomes:
- Initial three-mode matrix: **38.58 s**, 90 video frames each, 191 AAC packets at
  32 kHz, 285 at 48 kHz, and zero for video-only; all decoded and covered their tails.
- Expanded **48.80-s** matrix exercised **two successive clips per recording epoch**.
  Each mode reused the same acquisition, including across mode changes. FFprobe
  verified actual IDR/zero video start, negative AAC preroll, contiguous DTS/duration,
  selected mono AAC rate and final coverage. FFmpeg decoded both tracks with no errors:

  | Output | Clip | Video / AAC packets | Fragments | Bytes |
  |---|---:|---:|---:|---:|
  | 32 kHz | 1 | 90 / 190 | 2 | 1,331,523 |
  | 32 kHz | 2 | 127 / 267 | 3 | 1,910,906 |
  | 48 kHz | 1 | 90 / 285 | 2 | 1,276,171 |
  | 48 kHz | 2 | 126 / 397 | 3 | 1,840,515 |
  | Video only | 1 | 90 / 0 | 2 | 1,194,684 |
  | Video only | 2 | 123 / 0 | 3 | 1,732,323 |

- Final repeat **48.92 s** passed the same six clip cases with explicit AAC-LC
  profile checks and empty decoder error output (32-kHz AAC counts 190/265,
  48-kHz 285/398, video counts 90/126, 90/126 and 90/124).
- **60.08-s production live A/V plus recording:** one main acquisition,
  **9,314 video /2,976 audio RTP**, **2,856,960** decoded untrimmed 48-kHz Opus samples;
  recording produced **9,416,376 bytes /13 fragments** and completed locally. Recording
  stop left both live owners current; audio BYE then joined only its worker while
  video stayed active. All released source and codec charges returned to zero.
- **12.54-s HDS fence check:** **748,190 actual fMP4 bytes in four authenticated frames**,
  one acquisition, joined teardown and retained-output refusal after source disable.
  A preceding **12.55-s** run exposed a test expectation error: it expected zero source
  charges while retained source-bound leases still owned **1,576 metadata bytes**.
  The final check verifies zero only after releasing those escaped permissions;
  retirement never pretends retained memory has been freed.
- Local checks cover six-hour ±0.1% video-cadence drift with repeated 145-ms IDR
  serialization bursts, contiguous durations/no admitted frame loss, unsupported
  gaps/admission timeout, incremental/malformed MP4 boxes, parent-lease retirement,
  cancelled join ownership, and stop/drop during stalled Rust acquisition.

The obsolete recording command, child/parser/stderr lifecycle, timestamp bitstream
filters and their native executable test input are removed. `apply-settings --write`
now calls the same Rust recording pipeline, but that mutation-bearing CLI was not run.
JPEG snapshot scaling and ignored external decode checks still use the executable.

Validation passed: **49 library +56 application tests**, focused source-recovery and
phase-trend checks, all-target compilation, strict library Clippy, formatting, two
sampler tests and the pinned Nix package without the old native recording-test input.
One bounded self-review returned no findings. The existing metrics `fetch_update`
deprecation warning remains unrelated to this cutover.

These checks do **not** establish Home/iCloud recording, Apple priming or physical
lip sync, seek behavior, automatic continuation after duration/size cuts, whole-camera
privacy, allocator-wide RSS bounds or months-long outage/reboot recovery. The
configured video cadence must remain within the explicit recovery policy; unsupported
cadence changes fail rather than invent timing.

### Home playback report and HomeKit audio wire-clock correction

The user reports Home app playback works, pitch sounds correct and burned-in seconds
advance approximately correctly, but audio is silent roughly **3/5 of the time**.
This establishes some real client playback, not acceptable audio continuity, measured
lip sync, a sustained soak, or an identified Apple OS/client/build combination.

The Rust cutover had incorrectly treated its internal 48-kHz Opus clock as the HomeKit
wire clock at every negotiated rate. For a 16-kHz request, **960 wire ticks mean 60 ms**
while a 20-ms packet contains **960 decoded samples at 48 kHz**. Correct-pitch playback
with roughly two-thirds gaps is consistent with that mismatch. Existing
[go2rtc HAP code](https://github.com/AlexxIT/go2rtc/blob/master/pkg/opus/homekit.go)
explicitly uses the requested rate for its HomeKit timestamp exception. This is strong
diagnostic support. The user subsequently confirmed: **“that fixed the audio.”**

The application boundary now converts signed, unwrapped canonical offsets to the
negotiated rate before adding/wrapping the session RTP origin, and creates RTCP clock
pairs at that same rate. At 16-kHz/20-ms this is **320 RTP ticks**, with unchanged
960-sample decoded packets. No encoder/resampler changes, silence insertion, dropped
AUs, reanchoring, widened recovery budgets, or legacy reader fallback are introduced.

The user confirmed the camera was idle before serial, read-only checks:
- **13.05 s:** all twelve 8/16/24/48-kHz × 20/40/60-ms producer configurations decoded
  25 authenticated packets each. Tests now check negotiated-rate RTP spacing and at
  least two authenticated SRs against their NTP interval, not just decodability.
- **60.04 s:** one acquisition, **9,311 video /2,984 audio RTP**, **2,864,640** decoded
  48-kHz samples, 16-kHz RTP/SR timing, audio BYE/joined cleanup with video still active.
- Local clock checks cover signed preroll, preserved common-domain offsets, all packet
  durations, RTP wrap, 180-day arithmetic, invalid rates and the SR mapping.

Earlier fixed-48-kHz matrix runs remain valid codec/transport observations, but their
wire-clock assertion was wrong for this HAP boundary and did not prove Home playback.
Validation passed: **47 library +53 application tests**, focused recovery/phase-trend
checks, all-target compilation, strict library Clippy, formatting, two sampler tests
and the pinned Nix package. Bounded self-review returned no findings.
The checks alone did not deploy or establish listening quality. The subsequent user
confirmation establishes that the reported Home audio gaps were fixed; it does not
identify Apple OS/build, measure lip sync or constitute a sustained soak.

### Recording ownership before the reader replacement

At this earlier stage the recording reader/mux was **still the legacy FFmpeg process**.
It has since been replaced by the shared path described above. Its detached parser
and stderr tasks, signal-only stop, and unfenced HDS writes have been replaced with
joined ownership and recording-epoch/stream retirement gates. Revoke serializes with
the final nonblocking TCP syscall, after encryption/copying; bytes already accepted
by the kernel cannot be retracted. Pending writes wake on revoke and have a five-second
deadline. Cancellation/error leaves the writer poisoned so a new encrypted frame cannot
follow a truncated one. Snapshot/publication share one lock to avoid duplicate boundary
fragments. HDS admission bounds prepared sessions/connections; native parsing bounds box,
init, fragment and prebuffer bytes. These are not complete aggregate Rust history budgets.

Recording reconciliation reads current desired state under its own lock. The camera-active
callback now awaits recording reconciliation rather than acknowledging a detached task.
Application shutdown retires recording and joins HDS connections. Old stream acknowledgments
with a different stream ID cannot terminate the current owner or release its busy slot.
Those earlier checks did **not** establish complete camera-wide live privacy or Rust-source recording binding.

Local checks cover entered-write retirement ordering, copied-byte refusal, an unread TCP
peer, truncated-frame poisoning, stalled native reader stop/drop, and shutdown of an
unidentified HDS connection. Initial parallel/local and Nix runs hit an existing
immediate UDP rebind assertion's `AddrInUse`. Its weak-owner release assertion remains
immediate; port reuse now allows one second for descriptor-close scheduling during
concurrent native child startup. Nix also lacked the FFmpeg executable for the new
native-lifetime checks; it is now an explicit pinned `nativeCheckInputs` dependency.
Final parallel and serial suites passed: **47 library +52 application tests**, plus
focused recovery/phase-trend checks, all-target compilation, strict library Clippy,
formatting, two sampler tests and the pinned Nix package. Bounded review found no issues.

Read-only north-driveway check: the first **4.59-s** attempt authenticated recording bytes
but exposed the decoder's incorrect 32-bit OPACK length width (`unknown tag 0xf1`);
it is not a pass. Corrected 32-/64-bit string/data lengths and added large-value/truncated
length tests. The repeat **4.58 s** transferred **402,994 actual fMP4 bytes in three
authenticated HDS frames**, joined recorder stop, and refused retained old-fragment output.
No retained captures, encoder writes, Apple playback, iCloud storage or Rust recording
cutover are claimed by this check.

### Production live audio cutover

Shared main-encoder AAC now feeds joined, thread-bound Rust decode/resample/Opus workers.
Both tracks receive one session presentation clock and independent source-bound crypto
transports. Removed live audio's FFmpeg command/UDP relay, native child adapter/lifecycle
fixtures, timestamp rescaler and native sender-report extractor. Recording remains legacy.

Read-only checks on north-driveway, without retained captures or encoder writes:
- Initial production A/V: **2.43 s**, **222 video / 100 audio RTP**, **96,000** decoded
  untrimmed Opus samples at 48 kHz, 16-kHz/20-ms encoding, one acquisition. Audio BYE
  joined/released its worker while video stayed active; final source charges zero.
- An initial matrix attempt timed out after 8-kHz/20-ms; no diagnostic identified its
  cause. A logged retry passed eleven combinations then refused a **6,946-tick /
  144.7-ms positive arrival excursion** at 48-kHz/60-ms. Neither run is a pass.
- The explicit presentation policy now holds rate correction during positive arrival
  excursions ≤500 ms for <500 ms, without changing the anchor or discarding samples.
  Negative >100-ms, larger or persistent errors still fail. The historical candidate's
  limits are unchanged. Local tests cover catch-up and persistent-gap refusal.
- Final matrix **13.21 s**: all **8/16/24/48-kHz × 20/40/60-ms** configurations each
  delivered **25 authenticated, independently decoded** RTP packets with exact fixed
  48-kHz timestamp steps. One acquisition and released codec reservations.
- Production A/V **60.03 s**: **9,347 video / 2,980 audio RTP**, **2,860,800** decoded
  Opus samples at 48 kHz. Both tracks sent authenticated reports, one acquisition;
  audio BYE joined/released its worker without stopping video, then zero source charges.
- Final-code repeats: matrix **13.31 s** passed all twelve combinations; production
  A/V **2.30 s**, **225 video /100 audio RTP**, with the same teardown checks.

Startup explicitly counts decoder warmup/probe-backlog samples before selecting the
single anchor, bounded to two seconds. Complete live Opus packets include codec preroll;
no container skip command is assumed over RTP. No finite tail completion is claimed.
Six-hour ±0.1% drift simulation exercises the rate controller, not six hours of hardware.
These checks do not establish Apple playback, physical lip sync, long-run burst quality,
in-session outage recovery, compatible codec deduplication, recording or deployment.

Validation: **47 library +45 application tests**, recovery/phase-trend checks,
all-target compilation, strict library Clippy, formatting, two sampler tests and the
pinned Nix package passed. The bounded self-review returned no findings. Existing
application `fetch_update` deprecation warnings remain unrelated to this increment.

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
HDS ownership and the single production-reader cutover. C probe retirement was still
pending at that increment and is completed below; no new C shim or synthetic media
gate was introduced.

### Rust audio-processing increment

`tests/camera_audio.rs` fed **200 real main-stream AAC packets** through Rust-owned
FFmpeg AAC decode, fixed-ratio resampling/FIFO and libopus encode. Four processors
consumed the same source subscription, and a separate FFmpeg-library Opus decoder
checked each output. No C worker, external media process, camera write, capture file
or synthesized camera input was used. All configurations were mono.

The final run completed in about **4.45 seconds**. Each processor decoded **204800
source samples** (per channel at 48 kHz):

| Opus input rate / frame | Resampler samples | Opus packets | Decoded samples at 48 kHz | Source-timestamp refusals |
|---|---:|---:|---:|---:|
| 48 kHz / 20 ms | 204800 | 214 | 204800 | 213 |
| 16 kHz / 20 ms | 68267 | 214 | 204801 | 213 |
| 24 kHz / 60 ms | 102400 | 72 | 204800 | 72 |
| 8 kHz / 40 ms | 34134 | 107 | 204804 | 107 |

Decoded counts exactly matched the actual resampler output expressed in the decoder's
48-kHz domain after codec padding removal. The 16-/8-kHz finite resampling tails have
one/four additional 48-kHz-domain samples relative to the source count; these are
recorded, not a claim of source PCM equality or an accepted recording-tail tolerance.
The codecs are lossy and no listening/lip-sync or Apple-device evaluation was done.

The first lower-rate check exposed a padding-domain mismatch: native libopus encoder
skip/discard side data was in encoder-rate samples, while the downstream decoder used
48-kHz samples. Exact metadata conversion fixed the count mismatch; native metadata
is retained alongside the explicit decoder-domain accessor. No source clocks or PCM
samples were altered to fix that diagnostic.

All four processors observed **198 clock contradictions across 199 AAC boundaries**.
The first expected next sample PTS was **2080**, but the actual PTS was **1680**; observed
step errors ranged from **−400 to +128 source samples**. One AAC packet carried public
RTCP SR metadata. These short samples do not establish RTCP cadence or long-term A/V
mapping. The source-timestamp accessor refused contradictions/priming rather than using
private encoder sample ordinals as a substitute source clock. Codec output exists;
usable live audio transport timing remains unresolved.

An additional processor was intentionally left with held output until its bounded
packet allowance failed. That failure revoked its own outputs while the four healthy
processors continued. Job admission, retained-output charges, repeated cancellation,
source-disable revocation and final reservation release also passed. This checks
processing ownership, not compatible-encoder sharing, network backpressure or HDS.

All-target type-checking, `nix build .#amcrust --no-link`, the existing **44 application
Rust tests**, formatting and strict library-only Clippy passed for this increment.
The production audio reader/transport remains unchanged; clock acceptance and actual
compatible-processing sharing are still open TODOs.

### Rust source-clock characterization

`src/source/clock.rs` now observes each shared-source generation before packet
admission. `src/ingest.rs` exposes public `AVProducerReferenceTime` plus all five
`AVRTCPSenderReport` fields. The bounded snapshot contains numeric counters/ranges,
not retained media, URLs or a replacement clock. Malformed metadata is refused;
changed report SSRC, non-advancing/ambiguous report clocks and unexpected PRFT flags
retire the source before publication. Duplicate reports do not refresh report age.

`tests/camera_clock.rs` made **two read-only 60-/20-second generation pairs** against
**10.76.14.10**, main H.264 90-kHz / mono AAC 48-kHz source, on Darwin arm64 with the
same pinned bindings/libraries. Each pair disabled/joined its reader before reopening,
then joined shutdown even on a returned error/timeout. No camera writes, retained
captures, synthetic camera input, production consumer or Apple client was involved.
The physical model was not independently identified by this check.

| Run / window | Source AAC packets | Decoded AAC packets / samples | Distinct SRs per track | Final AAC duration debt |
|---|---:|---:|---:|---:|
| A / 60 s | 2822 | 2822 / 2889728 | 13 | 3504 ticks = 73 ms |
| A / fresh 20 s | 949 | 949 / 971776 | 5 | 1456 ticks = 30⅓ ms |
| B / 60 s | 2822 | 2820 / 2887680 | 13 | 1824 ticks = 38 ms |
| B / fresh 20 s | 949 | 949 / 971776 | 5 | 2512 ticks = 52⅓ ms |

Duration debt is **demux DTS elapsed minus accumulated declared durations**, not
measured lip-sync error or independent capture time. Actual decoding still yielded
exactly **1024 samples per consumed AAC packet**. Source-observation and consumer
windows need not have identical endpoints. Audio transport-timestamp assignment
continued to refuse contradictions/priming; no sample-count clock was substituted.

- Sender NTP report intervals spanned **4.144–5.060 s** across the four generations.
  Packet-attachment gaps spanned approximately **3.884–5.077 s**, including reader
  buffering/scheduling, not raw network RTCP arrival times. First attached metadata
  appeared **379–509 ms** after the open attempt; this is not startup/playback latency.
- Nominal RTP elapsed minus NTP elapsed between reports ranged approximately
  **−77 to +10 ms for video**, **−47 to +7 ms for audio**. Sender packet/byte counters
  are modular observations, not a loss count or proof of one RTP packet per AAC AU.
- In run B, AAC step errors on packets **without** SR metadata were **−400 to +128
  samples** in both windows. On SR-bearing packets they were **−160 to +656** in
  the 60-second window and **−160 to +2384** in the fresh 20-second window. Thus the
  largest observed AAC jumps there coincide with report boundaries; treating all
  errors as harmless millisecond rounding is not supported.
- PRFT appeared on every observed packet, including initially unclocked video.
  The inferred `(PRFT − PTS)` epoch spread was **0–1 μs for video** and approximately
  **333–334 μs for audio**; the first audio/video epoch difference was about **333 μs**.
  These are public-metadata relationships, not an independent A/V sync measurement
  or authorization to reconstruct missing PTS/DTS.

The pinned upstream implementation explains the provenance: in FFmpeg 9.0.2,
[`rtp_set_prft` / `finalize_packet`](https://github.com/FFmpeg/FFmpeg/blob/n9.0.2/libavformat/rtpdec.c)
use the latest SR and packet RTP timestamp for PRFT (`flags=24`) and multi-track PTS;
[`ff_rtsp_fetch_packet`](https://github.com/FFmpeg/FFmpeg/blob/n9.0.2/libavformat/rtsp.c)
propagates the initial NTP origin between tracks. An SR is attached to a following
packet, not a packet necessarily sampled at its RTP/NTP instant. Replacing PTS with
PRFT would therefore reuse the same evidence, not solve the continuous-audio problem.
No private FFmpeg structure is read by application code.

The owner now exposes report age, phase/rate observations and generation retirement,
but chooses **no freshness timeout, drift tolerance, smoothing or priming policy**
from these short samples. Clock-policy acceptance and recording presentation remain
unchecked in the TODO. Production readers/transports are unchanged.

All-target type-checking, **8 clock/metadata library tests + 44 existing application
tests**, formatting, strict library-only Clippy and the Nix application build passed.
The existing real acquisition, shared-source lifecycle, AAC/Opus and RTP/fMP4 checks
also passed with the new observer. The AAC regression saw a further **+3296-sample
step error** in its short window, reinforcing that observed maxima are not a chosen
tolerance. The unrelated `AtomicU64::fetch_update` deprecation warning remains.

### Explicit live-audio clock candidate

The selected path is implemented as opt-in `ClockPolicy::AnchoredSamples`, with
strict behavior retained by the existing constructor. It anchors once to decoded
source PTS at SR/PRFT-backed admission, then advances by actual decoded samples.
It checks every frame against that prediction and fails the processor above a
**100-ms phase budget** or **15-second SR-bearing-packet age**. These are provisional
operational refusal limits, not accepted lip-sync tolerances. Source packets and
mismatch statistics are unchanged. See Architecture for the alternatives and
symptoms that would require reconsideration; execution work remains only in TODO.
The candidate currently refuses configurations other than 48-kHz input/output and
20-ms Opus frames, isolating this decision from lower-rate finite-resampler tails.

`tests/camera_audio.rs::real_anchored_audio_clock` ran two read-only 60-second windows
on the authorized camera through one shared source. Parallel strict and deliberately
one-sample-budget processors consumed the same delivery. No capture files, camera
writes, listening test or HomeKit transport were involved.

| Window | AAC packets / decoded samples | Opus packets | Final / maximum absolute phase | Outcome |
|---|---:|---:|---:|---|
| First | 2819 / 2886656 | 3008 | 3072 / 3392 samples (64 / 70⅔ ms) | No budget trip |
| Final | 2822 / 2889728 | 3011 | 1152 / 2032 samples (24 / 42⅓ ms) | No budget trip |

- An independent Opus decoder's output count matched the advertised audible interval
  **for every packet**, and its final sample count equaled the actual decoded AAC
  count. Audible intervals were contiguous on the candidate timeline. This validates
  the explicit presentation contract, not physical A/V synchronization or PCM equality.
- The first attempt correctly refused an incomplete priming description: initial
  pre-skip was in **OpusHead**, with no packet skip side data. The contract now accounts
  for header pre-skip once, checks it against encoder delay, retains end discard and
  refuses unsupported shapes/coverage. `source_timestamp_48k` does not grant padded
  packets a raw transport timestamp. The independent decoder actually honored these
  boundaries; an RTP receiver without that contract is not covered by this result.
- The final strict processor retained **2820 clock contradictions** and **3009
  timestamp refusals**. The candidate did not erase that input evidence: its output
  clock is an explicit model rather than a claim that the original PTS became exact.
- The deliberately tight processor failed with `Clock`, revoked its held output and
  released reservations while the other processors continued. Source disable revoked
  candidate output; reopening produced a new unanchored processor, whose first frame
  established a fresh zero-phase state. Unsupported lower-rate candidate admission
  left no reservation. All reservations were released and reader shutdown was joined.

These two windows are not a long soak. The real test records an explicit budget-trip
outcome if the phase guard fires; such an outcome is **not successful clock acceptance**
and must not be hidden by restarting the processor. A longer qualification, actual
Apple onset/lip-sync behavior, live receiver preroll/late joins, lower-rate coverage,
and HSV cut/decoder-history acceptance remain outstanding.

The **11 library + 44 application tests**, all-target checking, strict library-only
Clippy, formatting and Nix application build passed. The existing four-configuration
AAC/Opus check and real shared-source/socket/lifecycle check also passed. Report-age
expiry was checked with controlled unit-test instants, not an induced real-camera
RTCP outage. No production or Apple playback acceptance is implied.

### Negotiated-rate timing and finite endpoints

The guarded live-audio clock now accepts 8/16/24/48-kHz encoder rates and 20/40/60-ms
frames from the same 48-kHz AAC source. These cover the rates/packet times used by
the existing HomeKit live path. Original packet metadata is not rewritten. Source
phase/freshness guards and strict-by-default selection are unchanged.

`presentation_48k` rescales encoder positions, durations and initial padding exactly
into the decoder/RTP 48-kHz domain. A successfully finished resampler may extend the
output endpoint by less than one encoder-rate sample: that excess is exposed as
`resampler_tail_discard_48k` and included in the effective decoder end trim. It is
not silently called captured audio. Pending/live cropping, undercoverage and larger
excesses are refused; local codec completion is never recording acknowledgment.

The read-only `real_negotiated_audio_clocks` check consumes one shared source with
bounded processors for all twelve rate/frame combinations. Windows of 199/200/201
AAC AUs exercise different resampler endpoint phases; an additional 200-AU case
covers the common 16-kHz/20-ms mode's successful finite endpoint. Each successful
case checks continuous source-anchored audible intervals, exact decoded coverage,
native timestamp/skip metadata preservation and final reservation release.

| Encoder rate / frame | AAC AUs | Source 48-kHz samples | Additional 48-kHz end discard / result |
|---|---:|---:|---|
| 8 kHz / 20 ms | 199 | 203776 | 2 samples discarded beyond source endpoint |
| 16 kHz / 20 ms | 199 | 203776 | **Incomplete**: resampler returned 67925 samples, covering only 203775 source-clock ticks |
| 24 or 48 kHz / 20 ms | 199 | 203776 | No extra discard |
| 8 kHz / 40 ms | 200 | 204800 | 4 samples discarded beyond source endpoint |
| 16 kHz / 20 or 40 ms | 200 | 204800 | 1 sample discarded beyond source endpoint |
| 24 or 48 kHz / 40 ms | 200 | 204800 | No extra discard |
| All four rates / 60 ms | 201 | 205824 | No extra discard |

The one-tick shortfall is explicitly refused, not repaired by silence, accepted as
a tolerance, or treated as an unsupported live mode. Failure revokes that job's held
output while the other processors and source continue. A caller needing an exact
finite window must handle the refusal; ordinary live cancellation must not synthesize
a successful `finish`. AAC passthrough recording cuts remain a separate problem.

The first per-packet check also exposed a diagnostic assumption: FFmpeg's native
Opus decoder buffered 24 SILK-resampling samples across callbacks at a lower rate,
so a callback count was not the coded packet's duration. Per-packet contract checks
now use the public FFmpeg **libopus decoder**, with the effective 48-kHz trims supplied
on a private packet copy. The existing native-Opus whole-window checks remain in
place. No production timing was changed to fit that decoder behavior, and no private
FFmpeg structure is accessed. These are sample-count/presentation checks, not PCM
equality, listening, Apple startup or sustained lip-sync acceptance.

Final checks passed: **15 library + 44 application tests**, all-target checking,
strict library-only Clippy, formatting, the negotiated-rate matrix, the existing
four-configuration audio check, the 60-second candidate check and shared-source
socket/lifecycle checks. The Nix application build passed with substitution disabled
for that invocation after the configured cache returned HTTP 500s/timeouts. The final
matrix had twelve fully covered completions and the one explicitly refused finite
shortfall. No HomeKit transport or legacy-reader removal is claimed by this increment.

### Shared secure transport in the production live path

`src/media/transport.rs` now replaces inline encryption/socket writes in both production
live proxies. It is the same API used directly by Rust packetizers, with no runtime
backend switch or fallback. Session end/replacement/suspend/shutdown revoke all affected
track transports before awaiting proxy abort/join. The gate covers the actual nonblocking
UDP write, not just an earlier buffer-access check. A producer predicate is rechecked at
that point, but source-generation privacy still needs owner-level transport revocation;
no source/recording privacy acceptance is inferred from a session fence.

Outbound RTP/SRTCP sizes include their 10-/14-byte protection overhead. One pending
send and one bounded feedback receive are allowed per transport, with no internal
queue and a 250-ms write-readiness deadline. Feedback must come from the fixed peer,
authenticate, pass a 128-packet replay window and retain the first authenticated sender
SSRC. Unknown unauthenticated SSRCs do not grow retained state. A changed authenticated
sender requires a new session rather than silently resetting replay protection.

The existing read-only `real_camera_media_output` check now sends **SRTP directly**
from two Rust H.264 packetizers to independent native FFmpeg decryptors/decoders with
fresh per-viewer keys. At **400-/1200-byte protected datagram limits**, both decoded
**30 frames**, from **3418/1136 RTP packets** over their admitted real-source windows.
No camera settings were written or media retained. The same run retained the existing
mux regressions: **91 video / 283 AAC packets**, **1,264,897 accepted AVIO bytes**, exact
shifted PTS/DTS, video-only decoding and local cancellation/resource checks. The **90
video / 282 AAC duration differences** remain unresolved presentation evidence, not
an accepted tolerance.

Focused checks passed: **22 library + 43 application tests**, all-target checking,
strict library-only Clippy and formatting. These include exact encrypted wire size,
RTP sequence rollover without timestamp changes, authenticated feedback/replay/peer
isolation, producer recheck, repeatable revocation, receive wakeup, panic-safe stop,
and the actual production adapter's legacy timestamp mapping, RTCP feedback forwarding
and joined socket release. The legacy rescaler remains confined to that adapter;
it must not be applied to the new Rust audio presentation clock. The Nix application
build also passed with substitution disabled; bounded self-review found no issues.

This is production **transport wiring**, not a deployed shared-reader replacement or
Home app acceptance. FFmpeg still acquires video/audio and generates their RTCP; Rust
source-clock reports/control handling, receiver priming, sustained A/V qualification,
source ownership and recording integration remain open in TODO. The native FFmpeg
receivers here do not establish Apple client behavior or a HomeKit audio trim contract.

### Retired experimental worker scaffolding

The obsolete experimental harness is removed: both C probe workers, `packet_wire.h`,
`nix/packages/ingest-worker-probe.nix`, and 52 Python fixture/AMC2/probe/helper/test
files. This also removes the probe package's direct json-c and synthetic-check inputs.
No project-owned `.c` or `.h` source remains. The Rust media library uses upstream
FFmpeg bindings; no replacement IPC layer, fixture milestone or selectable backend
was added.

The six numeric JSON records below and the consolidated findings are retained unchanged.
Historical script names identify the removed implementations; they are not runnable
commands or links to current files. The independent read-only production workload
sampler `scripts/ingest_baseline.py` remains, with its two existing contract checks
extracted into `scripts/test_ingest_baseline.py` before removing the mixed probe suite.

This completes the project-owned C/experimental-scaffolding cleanup only. Production
FFmpeg camera readers, their clock/RTCP adapters and recording ownership remain until
the Rust consumers replace them. No camera configuration or runtime media behavior
was changed by this retirement, and it establishes no new playback acceptance.

Checks passed: the two retained Python checks and a read-only sampler smoke check,
**22 library + 43 application Rust tests**, all-target checking, strict library-only
Clippy, formatting, offline flake checking and the Nix application build. Package-name
evaluation on aarch64-darwin, aarch64-linux and x86_64-linux showed only the probe export
removed; all six numeric records retained their original hashes. No camera check or
synthetic media experiment was run for this deletion-only runtime increment.

### Rust-owned outbound RTCP reports

`media::rtcp::Sender` now builds the production adapters' compound SR/SDES packets
instead of forwarding FFmpeg's report payloads verbatim. A fresh random identity per
prepared viewer session gives both tracks the same CNAME without exposing host/camera
names or the HAP session ID. SSRCs and accepted-write counters remain per track.
The NTP/RTP clock pair and report cadence still come from the legacy producer;
formatting preserves those bits and does not invent a source-to-NTP mapping.

`Transport::rtp_counts()` updates under the actual successful nonblocking write gate.
It excludes RTP headers, CSRCs, extensions, padding and protection overhead, wraps
counters modulo 2^32, and does not count refused or unsent packets. These are kernel
acceptance counts, not controller acknowledgments. Bounded local control parsing
rejects malformed compound/SR/BYE shapes, wrong sender SSRCs and ambiguous duplicate
reports. Other framed blocks, including FFmpeg's SDES, are not forwarded. BYE is
appended to an accompanying report; a bare BYE retires locally without fabricating a
clock or emitting unnegotiated reduced-size RTCP. Authenticated controller feedback
still returns to the legacy producer.

The production-adapter check decrypts the new report and verifies its explicit clock,
actual two-packet/two-payload-byte counts, session CNAME, feedback forwarding and joined
shutdown. Protocol checks cover counter rollover, header/extension/padding accounting,
compound lengths, common versus fresh identities and terminal BYE.

The existing real-camera SRTP regression passed with **30 + 30 decoded frames**,
**3402 / 1127 accepted RTP packets** at protected limits of **400 / 1200 bytes**;
accepted packet/payload counters matched the sent real-source RTP data. It also
retained the mux regression (**91 video / 285 AAC packets**, **1,252,367 AVIO bytes**,
exact shifted PTS/DTS, **90 / 284 duration differences** still unresolved). The first
attempt failed before decoding with a local FFmpeg UDP bind collision. The diagnostic
now reserves both viewers' even RTP/odd RTCP pairs and sender sockets before either
decoder starts, avoiding its own port competition during handoff; external port races
remain possible. No camera settings were written or media retained.

This is outbound report ownership, not source-clock qualification. The real-source
viewer check does not yet originate source-clock RTCP. Shared-source injection,
qualified NTP mapping/report scheduling, Rust feedback handling and audio priming still
block removal of the legacy adapters/readers. No Home app or A/V acceptance is claimed.

Final checks passed: **25 library + 43 application tests**, all-target checking, strict
library-only Clippy, formatting and the Nix application build (substitution disabled).
Bounded self-review found no issues.

### Sustained clock attempt and first-failure provenance

The explicit candidate check now accepts `AMCRUST_CLOCK_SECONDS=60..1800` (default
60). The 1800-second mode is a **single acquisition, 30 minutes or first refusal**:
no automatic reanchor/restart, unchanged 4800-sample/15-second guards, and a nonzero
test result on a refusal. It skips the default check's separate fresh-generation
lifecycle exercise. Cleanup/join still runs. Constant-space diagnostics log exact
phase samples, report boundaries/age and an online least-squares phase slope against
nominal decoded-input seconds. The floating-point slope is display-only, not an
oscillator measurement, source-clock correction or accepted tolerance.

The first 1800-second attempt **failed with `Source(Clock)` after 34.33 seconds total**
(including admission and cleanup). The last periodic observation was at **29.071 s**
inside the anchored window: **688 samples / 14⅓ ms**, a peak of **688 samples**, fresh
report age about **2 ms**, and diagnostic slope **0.268577 ms per nominal second**.
No candidate phase-budget trip was observed. This does **not** establish either that
the fixed-rate candidate is wrong or that it is qualified: the source generation
retired first. The original diagnostic collapsed metadata/admission clock refusals,
so the exact offending packet/track/cause was not retained and cannot be recovered
from that run. The qualification was not restarted or relabeled successful.

`Source::clock_snapshot()` now retains one first-failure record per generation:
metadata/admission fault kind, stream/sequence, original optional PTS/DTS, previous
DTS, duration, flags, byte count and public SR/PRFT values. Missing media timestamps
and non-increasing DTS receive distinct admission fault kinds. The record survives
retirement, cannot be overwritten by cleanup, resets for a new generation and retains
no packet bodies or credentials. Admission/retirement rules are unchanged. The
camera check now prints terminal source and candidate observations even when input
fails before the candidate processor, and asserts one acquisition in qualification mode.

One **separate 60-second diagnostic**, not a qualification retry, passed after this
instrumentation. It consumed **2821 AAC packets / 2,888,704 decoded samples**, emitted
**3010 Opus packets**, and the independent decoder matched **2,888,704 audible samples**.
Final phase was **256 samples / 5⅓ ms**, peak **864 / 18 ms**, and diagnostic slope
**0.231726 ms per nominal second**; the strict parallel processor refused **3008**
output timestamp mappings. No source fault occurred in that short window, so it did
not identify the earlier failure. Its existing fresh-generation lifecycle check then
opened a second generation and released all charges. This later short success does
not replace the failed sustained attempt or establish physical A/V sync/PCM equality.

The next decision needs a classified source refusal, not a larger audio budget or an
unjustified switch to asynchronous resampling. Use the first-failure record to
separate SR/PRFT provenance failures from actual missing/non-increasing timestamps;
keep source-clock recovery and Apple presentation acceptance open in TODO. Both runs
used 48-kHz AAC input and 48-kHz/20-ms Opus output, with coordinated read-only camera
access and no encoder writes or retained captures. Other negotiated modes and physical
A/V presentation are not qualified by these results.

Implementation checks passed: **26 library + 43 application tests**, the diagnostic
slope check, all-target checking, strict library-only Clippy, formatting and the Nix
application build (substitution disabled). Bounded self-review found no issues. These
checks and the separate short diagnostic do not turn the sustained refusal into a pass.

#### User-requested real-camera retry

At the user's request, the same 1800-second-or-first-refusal check was repeated on
real hardware at **10.76.14.10**, main encoder, with unchanged source admission and
candidate budgets. No local RTSP connection was present before starting. This does
not establish exclusive camera access: other hosts were not inventoried or stopped.
No camera configuration was changed and no media capture was retained.

This distinct attempt **failed with `Source(Clock)` after 200.19 s total**, Cargo exit
101. The new diagnostics captured **`NonIncreasingDts`** on AAC stream **1**, global
packet sequence **12370**, in the **1/48000** time base:

- Previous admitted DTS **9,598,224**; offending PTS/DTS **9,598,128**: **−96 ticks /
  −2 ms**, rather than the declared **+1024 samples**. Step-minus-duration was
  **−1120 samples / −23⅓ ms**.
- Packet size **284 bytes**, flags **1**, with a sender report attached: SSRC
  **1786523631**, NTP **17196193062787408199**, RTP **1741884926**, sender packet/byte
  counts **18745 / 5370311**. PRFT was **1794811899204999 µs**, flags **24**.
- Last candidate observation at **199.7599 s**: **9,596,928 decoded samples**, phase
  **2336 / 48⅔ ms**, peak **2512 / 52⅓ ms**, report age about **4.991 s**, diagnostic
  slope **0.044957 ms per nominal decoded second**. Neither phase nor freshness
  budget tripped. The newly attached report was observed by the source, but its
  offending packet was refused before publication to the candidate.
- Final source state: generation **1**, **one open attempt**, **one fault**, retired,
  inactive, zero subscribers and zero charged bytes. There was no acquisition restart.

This identifies the immediate admission refusal, not its ultimate origin. The
rollback coincides with an SR-bearing packet, but these observations alone cannot
separate camera RTP timing from libavformat's SR-based timestamp mapping, nor
independently establish capture timing. The user subsequently supplied service
status confirming `amcrust-north-driveway` on `salty-fight` had stopped at
**2026-11-16 06:40:21 UTC**, before the retry. That service is therefore ruled out as
the competing reader, rather than retained as an explanation. The earlier 34.33-second
failure still lacks an offending-packet record and must not retroactively be assigned
this exact cause.
Neither attempt qualifies the clock. Investigate the SR-boundary mapping without
clamping timestamps, widening budgets or treating PRFT as independent evidence.
No application code or timing policy changed for this retry.

#### AAC remapping classification and bounded subscription handling

Audited the actual pinned **FFmpeg 9.0.2** source, not private runtime structs:
`rtpdec.c:rtp_set_prft/finalize_packet`, `utils.c:ff_parse_ntp_time`, and the
MPEG4-GENERIC AAC depacketizer/header parser. PRFT inversion must match FFmpeg's
integer conversion exactly (including its UINT32_MAX fractional-NTP denominator),
round-trip to the original microseconds, and stay within the supported signed range.
This reconstructs the packet RTP timestamp from dependent public metadata; it is
**not an independent wire observation, packet-loss detector or capture-clock proof**.

Two read-only real-camera windows supplied adjacent-packet evidence:

| Window | Mapped DTS step | Reconstructed RTP step | SR mapping shift | Residual |
|---|---:|---:|---:|---:|
| Initial classification, stopped at **39.44 s** | −7488 ticks / −156 ms | +672 ticks / +14 ms | −8160 ticks / −170 ms | 0 |
| Focused source check, stopped at **154.39 s** | −6144 ticks / −128 ms | +1152 ticks / +24 ms | −7296 ticks / −152 ms | 0 |

Both had advancing report clocks and fresh report evidence. This shows that those
particular mapped rollbacks are explained by SR remapping, not a reconstructed media
RTP rollback. It does not retroactively identify the earlier unrecorded failure.
The initial implementation bounded a mapping update to 100 ms and refused both.
That conflated update size with endpoint phase error: valid +95-ms and −57-ms
endpoints differ by 152 ms. The final classifier instead permits at most the existing
±100-ms window's **200-ms diameter**, while the processor still refuses any individual
endpoint outside **±100 ms** and any stale report at **15 s**. These endpoint budgets
were not increased. A regression replays the exact 152-ms public metadata above,
without retaining audio or inventing media, and checks the unchanged endpoint guard.

The implemented source exception is explicit `Tracks::AudioSampleClock`, restricted
to audio-only consumption and AAC-LC/48-kHz/1024-tick timing. It requires exact public
metadata inversion, matching SR/PTS changes within one integer-rounding tick, positive
bounded reconstructed RTP progression, fresh advancing reports, unchanged SSRC,
PTS=DTS and uncorrupted packets. Original timestamps are never clamped or replaced.
Strict Audio/AudioVideo subscribers receive Clock termination and their held grants
are revoked before publication; video-only subscribers are unaffected. Missing clocks,
bad metadata and any non-increasing DTS outside this exception still retire the source.
First/last mapping records and counters are bounded and generation-scoped. Codec
sample progression, resampling and recording presentation policies are unchanged.

Additional hardware outcomes, recorded as separate attempts:

- A post-change **1800-second-or-first-refusal candidate attempt failed** at
  **97.72 s total / 97.363 s processing** with a genuine **phase-budget trip**:
  **4816 samples / 100⅓ ms**, fresh report age **3.122 s**, diagnostic slope
  **0.728512 ms per nominal decoded second**. There were no backward mapping steps
  in this window. The processor revoked held output; sibling audio/video remained
  live until explicit cleanup. This is not a successful sustained qualification.
- A separate **60-second diagnostic passed**, producing **3011 Opus packets** from
  **2822 AAC packets / 2,889,728 samples**, with independently decoded audible count
  **2,889,728**, final phase **144 / 3 ms**, peak **976 / 20⅓ ms** and no mapping
  rollback. Its subsequent fresh-generation lifecycle check also completed.
- The final focused source-only check watched for a real bounded rollback for
  **300 seconds**. **None occurred**: 14,053 AAC and 4496 video source packets,
  no source fault, one acquisition, zero charges after shutdown. It deliberately
  reported an inconclusive timeout rather than a successful exercise of the new
  exception. No audio processing/presentation was qualified by that metadata check.

At that checkpoint the classifier and subscription isolation had local regression
coverage, including captured real metadata, but the live exception still needed an
actual rollback occurrence. The subsequent section records that later occurrence.
The camera evidence also exposes an independent cumulative phase problem; repeated short successes do not solve it. No camera writes, captured
media, phase/freshness budget enlargement, automatic reanchor or production acquisition
cutover occurred. The next work remains in TODO, not a parallel experiment plan.

Implementation checks passed: **32 library + 43 application tests**, the phase-trend
check, all-target checking, strict library-only Clippy, formatting and the Nix build
(substitution disabled). Bounded self-review found no issues. These checks do not
replace the missing live rollback occurrence or resolve the failed phase qualification.

#### Live isolation and decomposition of accumulated phase

A subsequent real-camera 1800-second-or-first-refusal attempt **failed after
144.92 seconds total**, but exercised the bounded exception before that failure:

- At global sequence **5859**, AAC DTS repeated **4,548,864** while reconstructed
  RTP advanced **1152 ticks / 24 ms**. The report mapping shift was **−1152 ticks**,
  with **zero residual**. The strict Audio sibling terminated with Clock after
  **4440 packets**, last sequence **5858**, and its held delivery was revoked.
- Candidate audio continued, reaching **6,944,768 decoded samples**; video continued
  through sequence **8950**, on the **same generation and single acquisition**.
  Candidate output continuity was checked with the existing independent decoder.
  No physical playback/lip-sync acceptance follows from this.
- At sequence **8951**, mapped AAC DTS stepped **−12096 ticks / −252 ms** while
  reconstructed RTP advanced **+1152 / +24 ms**. The SR mapping shift was
  **−13248 / −276 ms**, residual zero: outside the unchanged **200-ms step cap**.
  The source correctly retired; last processed candidate phase was **3936 / 82 ms**,
  peak **4176 / 87 ms**, below its independent 100-ms endpoint limit. Final state
  was one acquisition, one fault, no subscribers and zero charged bytes.

This closes the previously unobserved live-isolation behavior, not sustained clock
qualification. The prior 300-second no-event check remains inconclusive in its own
right; the later occurrence is separate evidence, not a relabeling of that check.

Added constant-space `audio_progress` accounting to separate reconstructed media
RTP progression from mapped PTS and declared AAC duration. Fields identify the first
anchor and last sequence, packet count, elapsed values, duration debts, total mapping
change, rounded SR-predicted changes and unexplained residual. Missing/unusable
metadata freezes the prefix and records the first break; there is no automatic
reanchor. The initial diagnostic in the 144.92-second run froze at sequence 1, so it
supplies no usable long-window decomposition. The refined diagnostic retains
unexplained mapping residuals instead of demanding that all changes be SR-explained.
This is observational only: the runtime rollback classifier and all limits are unchanged.

A separate **60-second real diagnostic passed** with **2826 AAC packets / 2,893,824
samples**, **3015 Opus packets**, and **2,893,824 independently decoded audible samples**.
Its matched final endpoint used anchor **32**, source sequence **3728** and the same
2826 input packets as the processor; the source was not ahead at that endpoint:

| Diagnostic, 48-kHz ticks | Value |
|---|---:|
| Declared elapsed duration (between first/last packet starts) | 2,892,800 |
| Reconstructed RTP elapsed | 2,894,256 |
| Mapped PTS elapsed | 2,894,416 |
| Reconstructed RTP duration debt | **1456 / 30⅓ ms** |
| Rounded SR mapping changes | **144 / 3 ms** |
| Unexplained mapping residual | **16 / ⅓ ms** |
| Total mapped debt / candidate endpoint phase | **1616 / 33⅔ ms** |

The exact identity is **1456 + 144 + 16 = 1616**. Peak candidate phase was
**2496 / 52 ms**, diagnostic slope **0.272190 ms per nominal decoded second**,
with no mapping rollback in this short window; its separate fresh-generation
lifecycle check also completed. These are dependent metadata observations, not a
measured ADC oscillator rate or proof that no source audio was lost. They show that
SR updates alone do not explain this window's discrepancy; neither deleting SR
updates nor increasing the phase limit is a justified fix. All hardware checks were
read-only, serially coordinated, with no retained media or camera configuration writes.

Implementation checks passed: **33 library + 43 application tests**, the phase-trend
check, all-target checking, strict library-only Clippy, formatting and the Nix build
(substitution disabled). Bounded self-review found no issues. No clock policy or
admission budget changed in this increment.

#### Sticky audio quarantine for oversized explained mapping changes

The source now distinguishes **explanation** from **permission to continue**. The
same public-metadata checks can explain an AAC mapping change larger than the
200-ms continuation cap without granting audio any additional phase allowance.
Such a change quarantines the AAC-LC track for the generation: all existing Audio,
AudioSampleClock and AudioVideo leases are revoked before the offending packet could
be published, and new audio-bearing subscriptions fail with Clock. Video-only leases
and acquisition stay active. Audio does not silently restart/reanchor; only a new
source generation clears the quarantine (explicit owner reset, or fresh acquisition
after demand has ended).

`TrackSnapshot.quarantine` preserves first cause separately from whole-source faults.
Later quarantined AAC packets are discarded without updating timing observations,
but packet generation/index/global sequence checks still run. Video timing, native
reader failures, parameter changes, privacy/stop and shutdown are not bypassed.
This is failure containment, not clock recovery or protection from video's own faults.
Local regressions cover explanation versus continuation bounds, all audio grant types,
unaffected video grants, first-cause persistence, sequence checks across discarded
AAC packets and fresh-generation state. No endpoint/freshness/continuation limit grew.

Two separate real-camera source-only checks were run serially, read-only, with no
retained media or encoder writes:

1. **Failed after 204.30 s**, before an oversized audio event. H.264 stream **0**,
   sequence **12626**, had PTS/DTS **18,354,150** versus previous DTS **18,360,900**:
   **−6750 ticks / −75 ms** in the 90-kHz time base, declared duration **6000 ticks**.
   The packet carried an SR, but AAC-only reconstruction does not classify video;
   no raw video continuity is inferred. The generation correctly retired with
   NonIncreasingDts. Four smaller AAC remapping events had been observed without
   quarantine. Final state: one acquisition, one source fault, zero charges.
2. **Inconclusive timeout after 300.04 s**: no oversized audio event occurred. There
   was one smaller bounded AAC mapping event, no source fault, one acquisition and
   zero charges after joined cleanup. This is not a successful quarantine exercise.

The new ignored check `real_audio_quarantine_preserves_video` waits at most 300 s,
requires 30 video packets after quarantine, checks held-audio revocation and refusal
of new audio subscriptions, then explicitly resets and checks a fresh generation.
Those post-quarantine steps were **not reached** in these two runs. Runtime quarantine
and recovery coverage therefore remain open in TODO, as does video clock handling;
neither failure is relabeled a pass or hidden by changing bounds.

Local checks passed: 35 library and 43 application tests, the phase-trend test,
all-target checking, strict library Clippy, formatting and bounded self-review.
The pinned package built successfully with normal Nix substitution enabled. An
initial substitution-disabled attempt failed fetching `mailcap-2.1.54.tar.xz`
(HTTP 404) in the dependency closure; no pins were changed to bypass it.

#### Production receiver-feedback ownership and joined live teardown

Removed the production SRTCP-to-FFmpeg return path and separate live child-process
maps. `rtcp::Receiver` now validates bounded authenticated feedback transactionally,
retaining one participant's counters and last relevant SR/RR report. Malformed tails
cannot partly apply report state, consume a FIR sequence or trigger BYE. Signed loss,
sequence and raw jitter/LSR/DLSR are preserved without inferring RTT or playback.
SDES, PLI/FIR, generic NACK and BYE shapes/padding are checked; unknown framed blocks
are counted. Fixed-label metrics expose observations/refusals. Keyframe/retransmission
requests are not fulfilled by this parser and do not cause encoder writes or reanchors.

Each live media owner now supervises its temporary native producer. Producer exit
revokes/joins media; media termination kills/reaps the child and joins a fixed-buffer
stderr drain. Raw native diagnostic text is no longer retained/logged, avoiding
credential-bearing strings. Explicit stop/replacement fences affected transports
before awaiting owners; Drop signals cleanup but is not an acknowledgement. The live
video gauge follows transport-task lifetime, not stale child-map entries.

A **4.59-second real production video-path check** used north-driveway main input,
read-only and serially, through `StreamManager::start_stream`, the existing FFmpeg
video producer and Rust transport/control. A local authenticated protocol peer decrypted
**100 RTP packets** and an outbound SR, then sent RR, PLI and BYE. The media owner
finished and reaped the native child **without a HAP stop request**; explicit cleanup
left the live gauge zero. A subsequent **4.53-second repeat**, again decrypting 100
RTP packets and an SR, also checked successful owner join and that the process was
already reaped **before** invoking HAP cleanup. No encoder settings were written and
no media was retained.
This tests actual production control/lifetime, not H.264 decoding, Apple playback,
source timing qualification or shared acquisition. The test is ignored by default:
`stream::tests::real_production_feedback_and_joined_teardown`.

Local checks additionally cover producer exit, explicit joined stop/reaping,
no feedback datagrams forwarded to the producer, authenticated semantic refusal,
exact report fields, foreign identities/targets, FIR deduplication/wrap, malformed
compounds/padding and bounded parser mutation/truncation cases. Input and producer
clocks/cadence, live audio and recording still use the existing native paths. The
shared registry/producers, generation-to-transport fences, sustained clock recovery,
recording coverage/HDS ownership and HomeKit acceptance remain required before full
cutover or removal of the remaining FFmpeg command adapters.

Validation passed: 39 library tests, 44 application tests, the phase-trend test,
two retained Python sampler tests, all-target checking, strict library Clippy,
formatting and the pinned Nix package build. Bounded self-review found no issues.

#### Source-bound write fences and demand-driven recovery

`Transport::new_source_bound` now binds writes to the first delivery's generation
and subscription. Retirement synchronizes with the final nonblocking kernel write,
not just a cooperative producer predicate. Sticky notifications wake waiting I/O;
old transports cannot be rebound to a fresh generation. Local checks cover retirement
between predicate and syscall, entered-write acknowledgement ordering for generation
and subscription retirement, independent video during audio quarantine, and pending
feedback. These are library-boundary results, not production registry injection.

The read-only `camera_transport_fence::real_source_transport_fences` check passed in
**9.03 seconds** on north-driveway. Two viewers shared one acquisition, each sending
and authenticating **152 RTP packets** from a complete IDR. Dropping one subscription
closed its transport while the other remained usable. Source disable fenced copied
RTP and RTCP without explicit transport revocation. Re-enable created generation 2;
a fresh transport authenticated another **152 packets**, while the old one stayed
closed. Cleanup reported two acquisitions, no source faults, no subscribers and zero
charged bytes. This checks authenticated datagrams and lifecycle, not decoded video,
HomeKit playback or sustained recovery.

A separate **10.95-second** real output check switched both Rust H.264 viewers to
source-bound transports. Their receivers decoded **30 + 30 frames**, with **3414 / 1131**
RTP packets at 400/1200-byte wire limits. Dropping the subscriptions fenced copied
RTP without explicit transport revocation. The concurrent mux check retained exact
translated PTS/DTS for **92 video / 284 AAC packets**, with **1,268,445 AVIO bytes**;
MP4 duration differences remained **91 video / 283 AAC**, not presentation acceptance.
Library/application tests, the repeated-failure recovery test, phase-trend check,
all-target compilation, strict library Clippy, formatting, both sampler tests and the
pinned Nix package build passed. The existing application atomic-deprecation warning
remains. The external bounded self-review tool was unavailable; changes were reviewed
locally, including lock ordering and write/retirement races.

The source circuit breaker now permits fresh demand after a bounded cooldown rather
than remaining latched until an operator reset. A local TCP peer repeatedly closed
real native RTSP setup connections; three acquisition/cooldown cycles completed in
**1.32 seconds** with shortened test delays, no manual reset and no revived old leases.
Cooldown alone did not open a reader, and privacy disable continued refusing demand.
A subsequent **5.12-second** camera check failed two native setup attempts at a local
TCP endpoint, then relayed that same endpoint to north-driveway. Without source reset
or URL replacement, cooldown admitted generation 3 and a complete IDR produced **152
authenticated RTP packets**. Old subscriptions stayed retired; shutdown reported three
acquisitions, the two injected faults, no subscribers and zero charges. The initial
**5.04-second** run also recovered media but failed its overly strict relay-teardown
assertion on TCP `ConnectionReset`. The corrected check allows reset/broken pipe only
after media recovery succeeds and registry shutdown joins, and joins/aborts its relay
on deadline. Neither run is a camera-reboot, mid-stream outage, HomeKit reconnect or
months-long acceptance result; production recovery owners still need integration.

The user selected recovered presentation timing and bounded continuous audio
resampling for stable playback, preserving original camera clocks as diagnostics.
Unattended recovery over months, camera reboots and network failures is an explicit
requirement. This decision supersedes treating the strict source-anchored candidate
as the production clock contract; it does not turn its historical failures into passes
or claim that the recovered presentation implementation already exists.

#### Production shared-video cutover

`src/main.rs` now registers the native live encoders and supplies non-secret source
handles to `StreamManager`. `src/stream/shared.rs` owns per-viewer H.264 packetization,
source-bound writes, authenticated feedback, current host-NTP/RTP pairs and report
scheduling at the negotiated minimum interval (bounded to supported values). The
video FFmpeg command, video UDP relay sockets and native video timestamp adapter path
are removed; there is no runtime legacy video fallback. Audio/recording remain native.

Production sources explicitly select presentation timing. Camera timestamps and first
contradictions remain unchanged diagnostics; the video timeline uses monotonic packet
read instants instead. Structural packet identity, changed report identity, codec
changes, corruption, current SPS/PPS/slice references and negotiated dimensions remain
checks; B slices are refused instead of guessing display reordering. Initial missing
PTS/DTS is supported without fabricating source fields. A generation's timing mode
cannot change; strict audio/mux constructors reject presentation deliveries. Read-time
queue span, residence/count/byte limits and source-bound write fences stay enforced.
No capture-time accuracy, burst smoothing or common live A/V synchronization is claimed.

A real production `StreamManager::start_stream` check completed in **0.48 seconds**:
**153 RTP packets** and a Rust sender report were decrypted, RR/PLI/BYE were handled,
and the Rust producer ended without HAP stop. No video child process existed.
A separate application-producer sharing check completed in **0.52 seconds**, with
**100 / 154 authenticated RTP packets** and three B-viewer timestamps: both owners
used one acquisition and stopping A left B active. These are short control/lifetime
results, not timing benchmarks or Apple playback qualification.

The presentation-mode downstream decoder check passed in **6.92 seconds**. Two SRTP
receivers decoded **30 / 30 frames**, with **2670 / 901 RTP packets** at 400-/1200-byte
wire limits from one acquisition. The original `MissingMediaTimestamp` diagnostic
remained visible while presentation continued. Local checks cover unchanged rollback
metadata, B-slice/wrong-reference refusal, identity checks after a clock diagnostic,
180-day clock arithmetic/RTP wrapping, cancellation during stalled native RTSP setup,
late transport-install refusal and release of ended-owner sockets/source references.
The final production control repeat passed in **0.44 seconds** with **152 RTP packets**
and no video child. A strengthened sharing repeat waited for two fresh source video
packets after A stopped; it passed in **1.56 seconds**, with **100 / 47 authenticated
RTP packets**, three B timestamps and still one acquisition. Ended owners release
bound sockets/source descriptors without waiting for a later HAP cleanup request.

Validation passed: **45 library + 47 application tests**, recovery and phase-trend
checks, all-target compilation, strict library Clippy, formatting, two sampler tests
and the pinned Nix build. Bounded major-only self-review returned no findings. The
existing application atomic-deprecation warning remains.
These checks do not establish months-long operation, camera-reboot recovery, actual
HomeKit A/V, HSV cutover or aggregate production resource savings.

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
the final Rust architecture. Their implementations have now been removed; the names
in the table are historical provenance. **Do not schedule more synthetic testing for
the cutover.**

| Area | Established within the exercised cases | Important limit / former implementation (removed) |
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
