# Packet IPC to fragmented-MP4 A/V output

Status: **finite-file A/V reconstruction is proven for the fixtures below, with a
terminal-AAC-padding limitation. Raw unclocked RTSP-to-mux is deliberately blocked.**
The follow-up [shared-epoch clock proof](rtsp-clock-proof.md) now admits clocked
Baseline RTSP through bounded timestamp repair. Production acquisition is unchanged.

## Reproduce

```sh
nix build .#ingest-worker-probe --no-link -L
python3 -B -m unittest discover -s scripts -p 'test_*.py' -v
```

The existing opt-in Nix package now builds `mux-worker-probe` alongside the ingest
worker, using the flake's FFmpeg and json-c. Its check phase includes
`scripts/av_output_probe.py`, previous packet/RTSP integrations, and the unit suite.
No system-wide tools or production dependencies were added.

The probe generates temporary local media and a synthetic loopback RTSP session.
It never reads camera credentials or connects to a physical camera. Temporary
captures, MP4s and decoded media are deleted when the probe finishes.

## Reconstruction path

```text
Synthetic source MP4 → native ingest worker → framed packet IPC
    → native packet reconstruction → libavformat fragmented-MP4 muxer
    → packet/timestamp inspection + video/PCM decoding
```

The sink accepts exactly one generation, with descriptors before contiguous
packets and an explicit terminal packet count. It reconstructs codec parameters,
extradata, time bases, packet PTS/DTS/duration/flags and all side-data bytes. New
descriptor fields include channel layout, sample/pixel format, frame size, sample
aspect ratio and color metadata. They are still a supported subset, not a complete
portable serialization of AVCodecParameters.

json-c preserves integer timestamp precision; the sink additionally rejects
unsigned values that would saturate a signed-64-bit conversion. Both programs use
the same pinned libavcodec version; side-data enum IDs are not a cross-version ABI.

The output is a **new local file**, created exclusively. Existing paths are not
overwritten. Reported parse/mux errors remove partial output. An uncatchable process
kill can leave a partial file until the harness's temporary-directory cleanup;
there is no claim of production atomic publication or crash durability.

The sink never decodes/re-encodes video or audio. It uses:

- `frag_keyframe+delay_moov+default_base_moof`, with edit lists enabled;
- negative timestamp shifting disabled, rather than independently zeroing tracks;
- a movie timescale computed from the input clock denominators' LCM, with an
  integer-limit check, to avoid coarse edit-list rounding of relative track starts.

`delay_moov` lets the muxer see initial track timing before emitting initialization.
This differs from production's current `empty_moov` setup, but **production settings
were not changed**. Header latency and Apple compatibility still need measurement.

## Observed finite-file results

Environment: macOS arm64, flake FFmpeg 9.0.2, three-second fixtures.

| Property | Generation 1 | Generation 2 |
| --- | --- | --- |
| Video | 320×180, 15 fps, no B-frames | 640×360, 15 fps, two B-frames |
| Audio | AAC-LC, 48 kHz mono | AAC-LC, 32 kHz stereo |
| Requested source audio offset | 0 | 250 ms |
| Output video frames | 45 | 45 |
| Keyframe-aligned fragments | 3 | 3 |
| Observed PTS/DTS/duration error | 0 | 0 |
| Encoded packet payload hashes | Exact match | Exact match |
| Decoded video frame hashes/order | Exact match | Exact match |
| Decoded PCM | Exact source prefix + 384 terminal samples | Exact match |
| Independent direct FFmpeg copy control | Identical decoded output | Identical decoded output |

The timing comparison uses rational clocks, checking every packet independently
within at most one clock tick. It does not merely check total duration or permit a
separate arbitrary shift for each track. The observed error in both cases was zero.
The complete PCM prefix must match exactly, so front loss, drift, channel changes
or arbitrary sample differences cannot pass as terminal padding.

Each fragment is also decoded with its own matching initialization segment. Its
first video packet is a keyframe, and concatenating the independently decoded video
frame hashes matches the full clip. This covers video decoder independence at the
chosen fragment boundaries, not sample-exact independent AAC decoder startup.
Reassembling all init/media segments without the optional `mfra` seek index preserves
the full decoded A/V output.

### Terminal AAC padding: not silently waived

The first source file declares 384 trailing discard samples. Passing its side data
to libavformat preserves packet timing/duration, but the resulting streaming fMP4
does not retain equivalent terminal discard signaling for FFmpeg's PCM decoder.
It produces **8 ms of additional terminal samples**. A separate direct FFmpeg
packet-copy command with matching mux options produces exactly the same result.
This is a mux/container behavior, not evidence of lost bytes across IPC.

The test permits only those source-declared terminal samples, requires every source
PCM sample to match as an exact prefix, compares against the direct-copy control,
and reports the extra sample count. It does **not** claim lossless raw-PCM length
for that case. Logical clip-end handling/sample-exact finite output and HomeKit
playback behavior remain open. Continuous camera AAC does not carry the same
finite-file encoder-finalization metadata, so this needs explicit treatment rather
than a guessed global trimming rule.

## Generation and malformed-input guards

`split_recording` identifies initialization and `moof`/`mdat` pairs with bounded box
sizes, including extended-size headers. Each fragment carries its generation and
initialization hash. `assemble` accepts only consecutive fragments owned by that
initialization; a contiguous later subset is allowed for prebuffer-style clips.
It rejects mixed epochs, wrong init, gaps, duplicates and reordered fragments. These
are internal ownership guards, not authentication against forged media objects.

Fourteen native rejection cases cover mixed generations, absent/truncated endings,
oversized frames, sequence/count mismatches, DTS rollback, missing or nonintegral
PTS, unsigned timestamp overflow, invalid hex, non-keyframe video start, invalid
clocks and attempts to overwrite an existing file. Rejected new outputs disappear;
an existing file stays byte-identical.

The sink rejects missing timestamps or nonpositive durations; it does not synthesize
a decode clock or silently invent timing. It retains the earlier IPC size limits,
limits side-data totals/counts and rejects duplicate side-data types. It is an
experimental local consumer, not a hardened arbitrary-media service.

## Newly exposed RTSP blocker

The output proof also captures a deliberately bounded clip from the existing
loopback RTSP source while it is active. It then passes those **actual worker
packets**, without timestamp edits, to the sink.

With the pinned FFmpeg build:

- The first H.264 packet has no PTS; the first three have no DTS.
- Video durations are 6000 ticks on the 90 kHz clock.
- AAC has PTS/DTS and 1024-tick durations on its 48 kHz clock.
- The sink rejects this input specifically as **missing packet timestamp** and
  removes incomplete output. This is an expected guard result, not successful
  end-to-end RTSP A/V playback.

Filling each track from zero independently could alter relative A/V timing, and
using PTS as DTS is unsafe for reordered video. The [follow-up clock proof](rtsp-clock-proof.md)
adds shared-epoch RTCP fixtures and bounded Baseline startup repair. The original
unclocked case remains a rejection test; neither it nor file timestamps alone
establishes a correct real-camera synchronization policy.

## Next work

1. **Early-SR/Baseline subset complete:** [clocked RTSP → IPC → fragmented MP4](rtsp-clock-proof.md)
   preserves sender-schedule timing and decoded A/V, including bounded report loss.
   Broader profiles, late reports, production freshness budgets and reordered RTSP
   decode-clock recovery remain open.
2. Define finite AAC clip-end semantics and verify the chosen behavior in HomeKit;
   retain the direct-copy control and explicit padding assertion.
3. Binary IPC, time-based buffering, production supervision/leases and Rust
   integration. The current proof stages short finite captures and is not a sustained
   streaming/memory/throughput benchmark.
4. Both real camera models, Apple live/HSV acceptance and matched resource/latency
   measurements. None of those hardware gates are complete yet.
