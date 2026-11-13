# Shared-epoch RTSP A/V output proof

Status: **clocked synthetic RTSP now reaches packet-copy fragmented MP4 with exact
source-schedule timing and decoded A/V**, within the admission policy below.
Production acquisition, camera configuration and HomeKit transport are unchanged.
The earlier unclocked RTSP rejection case remains in the A/V output checks.

## Reproduce

```sh
nix build .#ingest-worker-probe --no-link -L
python3 -B -m unittest discover -s scripts -p 'test_*.py' -v
```

The package check phase runs `scripts/rtsp_clock_probe.py` with both native workers,
in addition to the earlier packet, RTSP lifecycle and finite-file output probes.
All media/endpoints are temporary synthetic fixtures on loopback; no credentials,
physical cameras or production services are accessed.

## Why sender-clock evidence matters

The original fixture supplied RTP timestamps but no RTCP sender reports. Its first
video PTS and some initial DTS values were missing. Filling each track from zero
would not establish synchronization: the initial timestamps are not sufficient
proof of a common epoch.

The extended fixture supplies compound RTCP **SR + SDES** with a common synthetic
NTP clock/CNAME, separate arbitrary RTP origins and correct packet/octet counts.
Reports precede each track's first media and recur every half second. Video's
origin deliberately crosses the 32-bit RTP timestamp boundary during the clip.
A staggered case sends audio's first SR at its delayed first media time rather than
at video startup.

The ingest worker now additionally exports:

- `hello.start_time_realtime`: libavformat's common source epoch, or null;
- stream `video_delay`;
- structured packet `rtcp_sr` fields: SSRC, unsigned 64-bit NTP and RTP timestamps,
  decoded from the public `AV_PKT_DATA_RTCP_SR` / `AVRTCPSenderReport` API.

Raw sender-report side data is still preserved too. There is no private FFmpeg
structure access or independent RTSP demuxer. FFmpeg remains responsible for mapping
RTP/RTCP into packet PTS. See its [RTP mapping](https://github.com/FFmpeg/FFmpeg/blob/master/libavformat/rtpdec.c)
and [shared RTSP epoch handling](https://github.com/FFmpeg/FFmpeg/blob/master/libavformat/rtsp.c)
for implementation background; results below use the pinned FFmpeg 9.0.2 build,
not an assertion that every version behaves identically.

## Bounded admission and repair

`scripts/rtsp_clock.py` is a streaming **experimental policy**, not a production
clock normalizer:

1. Validate the existing IPC generation, sequence, configuration and frame bounds.
   Require a non-null shared epoch and sender-report evidence on each track's first
   packet. First reports must fall within the half-second startup window relative
   to that epoch. A report arriving only after a track has emitted media is rejected:
   preceding packet timestamps may have used an unrelated local origin.
2. Hold startup output until every track has clock evidence and usable PTS. Retention
   is capped at **2 MiB of serialized frames, 64 frames, half a second of media per
   track and a four-second startup deadline**. The capture driver applies read
   deadlines too; a silent worker cannot hold admission indefinitely.
3. Repair only an initial missing-timestamp prefix of H.264 Baseline/Constrained
   Baseline, or the explicit zero-reorder Main/High subset described below, with
   declared `video_delay == 0`. Infer missing leading PTS backwards
   from the next known PTS using supplied positive durations. For this explicitly
   non-reordering profile, missing initial DTS can equal PTS. Each missing prefix
   has an eight-packet cap; known timestamps are never overwritten.
4. Validate strictly increasing DTS. Non-reordering video and AAC must retain
   contiguous packet timing. Missing timestamps after the known prefix, unsupported
   profiles, contradictory timing, gaps and rollback close admission.
5. Keep valid reordered-video PTS/DTS unchanged. Missing reordered DTS is rejected,
   **not** guessed from PTS or packet order. This branch has unit coverage and the
   existing finite-file B-frame mux proof, not a new B-frame RTSP integration claim.
6. Subsequent reports must retain SSRC and a consistent RTP/NTP slope, with modular
   32-bit timestamp arithmetic and at most one track tick of disagreement. Clock
   jumps, reversed/repeated report time or implausibly large report intervals fail.
   This is not adaptive drift correction. The freshness guards below additionally
   bound how long previously accepted reports may support output.

On failure the normalizer clears held packets/configuration and cannot be resumed;
a fresh generation needs a new instance. This is local rejection behavior. The
packet/RTSP probes now have [subscriber terminal signals](packet-ingest-proof.md#subscriber-terminal-signals),
but these are not yet integrated into a production supervisor.

The probe captures a deliberately finite clip while the source is still active,
then supplies an explicit matching end count. Source disconnection is never turned
into successful finite-media completion.

## Late first reports: explicit refusal, not guessed synchronization

The probe now exercises actual reports arriving **after** media in two loopback
cases: both tracks defer their first SR until one second, or only audio does so
while video reports from startup. Audio media starts at 250 ms in both cases.

Observed with the pinned worker:

| Case | Packets before each track's first SR (video / audio) | Admission result |
| --- | --- | --- |
| Both first reports late | 14 / 36 | Missing shared source epoch |
| Only audio's first report late | 0 / 36 | First packet lacks sender-clock evidence |

In both cases, the first AAC packet initially has **PTS 0**, while the independent
sender schedule places it at **12000/48000 = 250 ms**. Before clock mapping is
established, a valid numeric timestamp may be track-local rather than share an A/V
origin. A video-derived global epoch alone does not establish the audio mapping.

The harness keeps reading the source after admission refusal until both actual SRs
have been observed. It asserts that no startup frames were released, retained state
was cleared, and attempting to feed a later report cannot revive the same instance.
This proves the conservative refusal policy; it does **not** recover late-report
camera playback. Recovery would need a separately proven admission boundary and
clock mapping, not retroactive zeroing or silent rewriting of previously known PTS.
The source epoch diagnostic is explicit rather than a generic integer-schema error.

## Main/High reorder evidence

`scripts/h264_reorder.py` extends repair eligibility using the bitstream's declared
constraints, **not** an observed absence of B-frames or `video_delay == 0` alone.
It requires all of the following:

- Exactly one SPS/PPS pair in Annex-B codec extradata, profile ID 77 (Main) or 100
  (High), matching the worker's profile and dimensions.
- Progressive 8-bit 4:2:0 with POC type 0 or 2, and a well-formed SPS ending.
- Present VUI bitstream restrictions with **`num_reorder_frames == 0`** and consistent
  reference/decoded-picture-buffer bounds. Missing restrictions are unknown, never
  silently interpreted as zero or inferred from the level.
- Integer `video_delay == 0`, plus known packet PTS/DTS equality and contiguous timing
  under the existing no-reordering policy. A known nonzero composition offset is
  rejected rather than overwritten.

The evidence reader bounds configuration size to 16 KiB, SPS/PPS size to 4 KiB each,
NAL count to 256, Exp-Golomb code length, dimensions, HRD counts and scaling-list
loops. It handles VUI optional fields, HRD, High-profile scaling lists, and emulation
prevention before reading the restriction field. It is **not a full AVC validator**:
PPS and slice parsing covers only the identifier/type prefixes needed for this policy;
compressed picture decoding remains libavcodec's responsibility. The syntax's
[FFmpeg parameter-set implementation](https://github.com/FFmpeg/FFmpeg/blob/master/libavcodec/h264_ps.c)
is a useful reference, but this probe does not access private FFmpeg structures.

For admitted Main/High evidence, every packet must remain consistent: any announced
SPS/PPS must match the original bytes, slices must reference the admitted PPS, and
B/SP/SI slices or unsupported NAL types are rejected. A changed parameter set closes
the generation instead of inheriting its earlier no-reordering permission. The
normalizer clears this evidence along with its other state on failure.

Unsupported formats (including AVCDecoderConfigurationRecord/length-prefixed packet
inputs), multiple parameter sets, interlaced/other chroma or bit-depth variants,
POC type 1, absent bounds and positive reorder bounds **grant no new repair permission**.
Existing fully timed reordered input can still pass unchanged through the earlier
policy; this is not a new reordered-RTSP decode-clock recovery algorithm.

Real x264 Main and High no-B fixtures each preserve sender-schedule timing and decoded
A/V through RTSP and fMP4, with delayed audio and staggered initial sender reports.
A real x264 B-frame configuration reports reordering both through ffprobe and this
SPS reader, and denies repair even with a deliberately false `video_delay=0` candidate.
That negative case inspects configuration only; the fixture's RTP schedule still
assumes non-reordered access units. An actual in-session Main→High SPS replacement
over RTSP is separately rejected before output finalization.

## Report freshness and bounded loss

The initial implementation checked report consistency but could keep an old mapping
alive indefinitely if reports stopped. The normalizer now enforces a configurable
`report_timeout` (default **2 seconds**, positive and at most 30 seconds) using three
independent checks:

- **Receiver monotonic time:** each track has its own deadline from when its latest
  report was accepted from IPC. Other tracks' reports cannot renew it. `next_deadline`
  and `expire()` let the capture driver arm an idle timer, including after startup;
  silence does not require another packet to trigger expiry.
- **Shared media progress:** the furthest validated presentation end across tracks
  must remain within the budget of every track's latest SR NTP time relative to the
  common epoch. A burst of buffered packets, or a healthy track continuing while
  another stops, cannot hide stale clock evidence by arriving quickly.
- **Successive SR intervals:** a fresh-looking report arriving in a burst cannot
  bridge an already excessive sender-time gap. Intervals at or beyond the configured
  budget are rejected before renewing that track's mapping.

Expiry at the boundary closes the generation and clears retained state. A late
report does not revive it. The receiver clock must be finite and nondecreasing.
These are evidence-age limits, **not** estimates of camera-to-viewer latency: IPC
observation time is not the original network-receive/capture time.

The fixture emits SRs every 0.5 seconds, so the integration probe uses a **1.25-second
budget**. Losing one scheduled report is tolerated: the next arrives with a
one-second sender-time interval. That case still preserves exact packet timing,
video and PCM through fMP4 output. Sustained loss of all reports, loss of only video
reports while audio reports remain healthy, and post-admission socket silence each
fail specifically as report-freshness expiry. Worker cancellation/reaping still
runs on every capture failure.

Real cameras may report much less frequently. Neither the default nor the fixture
budget is a production recommendation; cadence, jitter and acceptable uncertainty
need coordinated hardware measurement. No automatic timeout extension or drift
correction is implemented.

## Observed results

macOS arm64, repository FFmpeg 9.0.2:

| Case | Initial reports | Video / AAC packets | Missing PTS / DTS repaired | Fragments |
| --- | --- | --- | --- | --- |
| Aligned A/V | Both at shared epoch | 30 / 94 | 1 / 3 | 2 |
| Audio starts 250 ms later | Both at shared epoch | 30 / 83 | 1 / 6 | 2 |
| Audio starts 250 ms later | Audio SR also starts 250 ms later | 30 / 83 | 1 / 6 | 2 |
| Aligned A/V, one scheduled SR lost per track | Both at shared epoch | 30 / 94 | 1 / 3 | 2 |
| Main, audio starts 250 ms later | Audio SR also starts 250 ms later | 30 / 83 | 1 / 6 | 2 |
| High, audio starts 250 ms later | Audio SR also starts 250 ms later | 30 / 83 | 1 / 6 | 2 |

For all six cases:

- Every normalized timestamp matches an **independent sender-schedule oracle**:
  video advances by 6000/90000 seconds; audio by 1024/48000 seconds plus its offset.
  The normalizer receives neither that offset nor the fixture's RTP origins.
- Every previously known timestamp, encoded payload and side-data field is unchanged.
- Every output MP4 PTS/DTS/duration matches the normalized input exactly, including
  relative track starts and the RTP wrap.
- AAC packet hashes and decoded PCM match the generated ADTS source prefix exactly.
- Decoded video frame hashes/order match the generated H.264 source prefix exactly.
  Annex-B → MP4 length-prefixed NAL representation is expected; this is packet copy,
  not video transcoding.
- Reassembling generation-bound initialization and fragments preserves decoded A/V.

An actual on-wire 200 ms video sender-report clock jump is rejected specifically as
a clock/SSRC discontinuity. Unit cases additionally cover missing epoch/evidence,
late/out-of-window reports, SSRC changes, timestamp gaps/rollback, bounded prefix
repair, buffer/deadline limits, stale generations and unambiguous B-frame timestamps.
Freshness regressions cover independent per-track renewal, silent startup/active
expiry, zero-wall-time media bursts, late renewal attempts and invalid timer inputs.

## Post-admission subscriber budgets

Successful normalized captures now also pass through a byte/count/media-duration
queue with unchanged wire bytes. A healthy consumer drains each frame; a separate
stalled consumer detaches on the synthetic 0.5 s media-span limit. This covers the
aligned and delayed-audio cases, one lost report, and the non-reordering Main/High
cases. The existing mux, timing, payload and decode comparisons remain in place.
Raw uncertain clocks are not admitted to this queue. See the
[duration-budget contract and limits](packet-ingest-proof.md#media-duration-budgets-after-clock-admission).
This replay check is not a concurrent live load or latency measurement.

## Remaining gates

- Real-camera sender-report cadence, NTP quality, late-initial-report recovery and
  source drift. Late reports are explicitly refused rather than guessed. Bounded
  report loss/freshness now has synthetic coverage, but suitable
  production budgets remain unmeasured. The current policy intentionally rejects some
  otherwise usable cameras rather than silently constructing an uncertain epoch.
- Broader H.264 configuration representations and reordered RTSP decode-clock
  recovery. Main/High now has the explicit SPS/VUI subset above; `video_delay == 0`
  alone still never authorizes its timestamp repair.
- The [finite-file terminal AAC padding limitation](av-output-proof.md) is unchanged.
  These clips end before the continuously sent AAC stream's encoder finalization;
  their exact PCM result does not solve finite-file tail-discard signaling.
- Binary IPC, duration-bounded subscriber queues, worker/supervisor integration and
  end-to-end sustained memory/load measurements. This proof bounds clock-admission
  retention, not all buffering inside libavformat or the finite-capture test harness.
- Both camera models and Apple live/HSV playback, latency/resource baselines and
  actual recording delivery acceptance. No hardware or performance claim is made.
