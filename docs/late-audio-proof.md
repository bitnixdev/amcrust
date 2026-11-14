# Late AAC and finite mux boundaries

The next output proof exposes two distinct start-boundary issues rather than
claiming late audio is solved: a **demux-order audio gap** and **decoder-history
sensitivity**. Production ingest, camera settings and Apple clients are unchanged.

## Experiment

`scripts/late_audio_probe.py` uses actual native-worker loopback RTSP captures,
shared-epoch normalization, and `LateAdmission` delivery leases. It intentionally
cuts a finite capture after 30 video frames and at least 80 AAC packets, as in the
existing clock proof. It replays that capture, joins at the second video IDR, and
builds a private finite mux input. This is not continuous acquisition, a real
recording trigger/end, or a performance measurement.

Four cases cover H.264 Main with aligned/delayed audio, High with delayed audio,
and a Main control with the synthetic AAC encoder's perceptual noise substitution
(PNS) disabled. Delayed cases use 250 ms audio offset and staggered initial RTCP
reports. No original source clock is independently zeroed.

All cases have identical AAC configuration: `1188`, AAC-LC, 48 kHz mono,
1024-sample frames, time base 1/48000. The pinned FFmpeg encoder's `-aac_pns`
option defaults to true; only the fourth synthetic control disables it. This is
not a camera configuration recommendation or a new supported-codec permission.

## Finite adapter contract

`scripts/late_mux_input.py` consumes only its admission's active delivery leases.
It verifies source identity, generation, consecutive original ordinals/sequences,
and the original source end count. It does not manufacture completion on live
EOF or allow an incomplete/retired input to finish. Finalization checks lease
expiry synchronously, including when timer dispatch is delayed.

The existing native sink expects a complete IPC stream starting at counter zero.
Only **private sink IPC sequence/end counters** are rebased. Source frames,
shared cursors, snapshots, codec metadata, timestamps, durations, payloads and
side data remain unchanged. Passing the unadapted suffix directly to the sink
continues to fail with `packet gap/duplicate`, without leaving a partial file.
This is not a reset of published source counters, RTP sequence numbers or clocks.

Defaults bound retained input to 256 packets and 2 MiB of the larger of encoded
AMC2 or inspection-JSON sizes per frame, including configuration/end overhead.
These are representation/count limits, not total heap/RSS or residence bounds for
copied output. Refusal clears the adapter's unfinished buffer and cancels only its
subscription. Already copied data is not magically revoked when the source closes:
callers must abort unfinished work; successful finish transfers ordinary diagnostic
copies. This is not a production privacy, recording lifetime or HDS delivery API.

The native full-capture mux is the packet-copy control. The late MP4 must retain
exact source PTS/DTS/duration, and its encoded packet hashes must equal the matching
suffix of that full mux. Comparing video in the same container representation
avoids confusing Annex-B to AVCC conversion with payload loss. Independent video
decoding must match original elementary-stream frames 15–29.

## Observed start gaps

Recorded on Darwin arm64, Python 3.14.10 and FFmpeg 9.0.2. All cases have 15 decoded
video frames, 44 AAC packets, and 45,056 decoded mono audio samples. Video starts
at original PTS 90000/90000 = 1 second.

| Case | First global sequence | First AAC source index / PTS | Audio starts after video | Audio ends after video |
|---|---:|---:|---:|---:|
| Main, aligned, default PNS | 65 | 50 / 51200 | 1/15 s (3200 samples) | 2/375 s |
| Main, 250 ms offset, default PNS | 54 | 39 / 51936 | 41/500 s (3936 samples) | 31/1500 s |
| High, 250 ms offset, default PNS | 54 | 39 / 51936 | 41/500 s (3936 samples) | 31/1500 s |
| Main, 250 ms offset, PNS off | 54 | 39 / 51936 | 41/500 s (3936 samples) | 31/1500 s |

A packet-order suffix at the IDR is therefore **not** an audio interval beginning
at that IDR. No silence, trimming, clock shifting, or hidden historical audio was
used to close these gaps. The end differences are whole-AU capture boundaries,
not newly inferred decoder padding. Earlier finite-file terminal AAC padding
limitations remain separate and unresolved.

## AAC decoder controls: exact differences stay visible

The original ADTS headers and payloads are copied unchanged into fresh-decoder
controls. Every captured AAC payload must equal the corresponding elementary
source access unit. The full MP4's PCM must equal the continuously decoded
source prefix. The late MP4's PCM must equal the independently decoded ADTS
suffix **exactly**, including its sample count: the container is not introducing
extra silence or trimming.

That fresh suffix is not identical to the corresponding continuously decoded PCM:

| Case | Fresh differing samples | Largest absolute signed-16 error | Differing samples after one preceding AU | Largest error after one AU |
|---|---:|---:|---:|---:|
| Main aligned, default PNS | 2001 | 3999 | 980 | 1 |
| Main delayed, default PNS | 2062 | 5014 | 1015 | 1 |
| High delayed, default PNS | 2062 | 5014 | 1015 | 1 |
| Main delayed, PNS off | 1022 | 5014 | 0 | 0 |

Default-PNS controls with **1, 2, 4, and 8 preceding AUs** still differ, including
near the end of the 45,056-sample target interval. Maximum residual error is one
signed-16 sample-value unit in these measured warmup controls; it is reported,
not accepted as an equality tolerance. Decoding the entire preceding history
(50 or 39 AUs) restores exact target PCM. This does not establish that the entire
history is necessary or identify a universal minimum preroll.

With PNS disabled, the fresh difference is restricted to the first 1024 samples,
and one preceding AU restores exact target PCM. Changing this fixture encoder
setting changes the result despite identical configuration descriptors. A blanket
“one AAC packet of preroll restores continuous PCM” contract is therefore not
supported. No assertion about corruption, audibility, specification conformance,
or Apple decoder behavior follows from these same-FFmpeg decoder comparisons.

Warmup PCM is sliced **only for analytical comparison at known AU boundaries**.
The actual late mux output is never trimmed, padded, replaced, or granted invented
skip-sample metadata. See [numeric observations](late-audio-results.json) for exact
mismatch counts, extents and all warmup windows.

## Validation and remaining decisions

The opt-in Nix check runs all four native cases, including the four unadapted-suffix
rejections. Unit coverage includes private-counter preservation, skipped/duplicate
receipts, foreign subscriber leases, released/retired input, incomplete/trailing
end, expiry without timer dispatch, local byte/count caps, strict fixture switches,
ADTS header preservation and exact PCM diagnostics. Full integrations, 160 Python
tests and 61 Rust tests/doctests passed.

Next work must distinguish packet continuity from PCM-exactness requirements and
explore a bounded, timestamp-selected AAC window covering the video IDR, rather
than a delivery-order suffix. Such a window also needs ownership/expiry and an
explicit common clip start/edit policy. The observed PNS-dependent differences
must remain explicit; this proof authorizes no generic preroll amount, arbitrary
sample removal, production recording adapter, hardware support, or Apple live/HSV
acceptance claim.
