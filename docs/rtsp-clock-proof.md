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
   Baseline with declared `video_delay == 0`. Infer missing leading PTS backwards
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
   This is not adaptive drift correction or a complete RTCP freshness policy.

On failure the normalizer clears held packets/configuration and cannot be resumed;
a fresh generation needs a new instance. This is local rejection behavior, not yet
a production supervisor/subscriber terminal-notification implementation.

The probe captures a deliberately finite clip while the source is still active,
then supplies an explicit matching end count. Source disconnection is never turned
into successful finite-media completion.

## Observed results

macOS arm64, repository FFmpeg 9.0.2:

| Case | Initial reports | Video / AAC packets | Missing PTS / DTS repaired | Fragments |
| --- | --- | --- | --- | --- |
| Aligned A/V | Both at shared epoch | 30 / 94 | 1 / 3 | 2 |
| Audio starts 250 ms later | Both at shared epoch | 30 / 83 | 1 / 6 | 2 |
| Audio starts 250 ms later | Audio SR also starts 250 ms later | 30 / 83 | 1 / 6 | 2 |

For all three cases:

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

## Remaining gates

- Real-camera sender-report behavior, NTP quality, late initial reports, report
  freshness/loss and source drift. The current policy intentionally rejects some
  otherwise usable cameras rather than silently constructing an uncertain epoch.
- Non-B guarantees for Main/High profiles (e.g. reliable SPS restrictions), and
  reordered RTSP decode-clock recovery. `video_delay == 0` alone is not treated as
  sufficient proof to replace DTS with PTS for those profiles.
- The [finite-file terminal AAC padding limitation](av-output-proof.md) is unchanged.
  These clips end before the continuously sent AAC stream's encoder finalization;
  their exact PCM result does not solve finite-file tail-discard signaling.
- Binary IPC, duration-bounded subscriber queues, worker/supervisor integration and
  end-to-end sustained memory/load measurements. This proof bounds clock-admission
  retention, not all buffering inside libavformat or the finite-capture test harness.
- Both camera models and Apple live/HSV playback, latency/resource baselines and
  actual recording delivery acceptance. No hardware or performance claim is made.
