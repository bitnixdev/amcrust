# Common-origin clip presentation controls

`scripts/clip_presentation.py` compares private mux presentations without changing
shared-source publications or choosing a production playback policy. The starting
point is an actual committed [recording-local cut](recording-cut-proof.md): nine video
frames (15–23) and 30 whole AAC AUs, including the selected historical prefix.

## Exact translations, not independent track origins

`translate()` consumes bounded finite diagnostic input, copies it and returns a
`Presentation(origin, frames)`. It subtracts **one exact rational origin** from every
track's PTS and DTS. Duration, payload, side data, flags, counters and configuration
remain unchanged. Original caller input remains untouched. Returned dictionaries are
ordinary independent diagnostic copies, not immutable source grants.

- **Earliest:** minimum selected PTS/DTS expressed in the common source epoch. In the
  exercised no-reorder clips this is the first selected AAC AU.
- **Video:** first admitted video packet's PTS, placing the IDR at zero and retaining
  negative AAC timestamps for the complete leading AUs.

An origin must be exactly representable on **every** declared track clock. There is
no rounding, independently resetting tracks, inventing missing clocks, or dropping
negative packets. Unknown clocks, missing finite end/tracks, non-key video start,
nonrepresentable origins and translated signed-native timestamp overflow are refused.
Input is capped at 266 records and 4 MiB encoded framing/body bytes; inspection copies
and encoding intermediates are separate representations, not a 4-MiB RSS guarantee.
This is not a generic decoder/preroll validator or production authorization API.

The returned rational origin is necessary to map private timestamps back to source
time. Hello/configuration metadata remains source provenance; translated frames must
not be republished through shared-source queues as if their clocks were unchanged.

## Native mux/edit observations

`scripts/presentation_probe.py` obtains its inputs through paced native recording cuts,
then compares presentations **after acquisition ends**. Presentation work is not routed
through the asynchronous output job and does not change its default exact-clock policy.
No new mux setting or C code was needed: the existing sink already uses a **720000-Hz
movie timescale**, the exact common multiple of the 90000-Hz video and 48000-Hz audio
clocks, with `delay_moov`, edit lists and negative timestamp shifting disabled.

Three synthetic cases use 320×180 progressive Main/High, AAC-LC 48-kHz mono:

| Fixture | Earliest origin | Video delay after earliest origin | Leading AAC before video origin |
|---|---:|---:|---:|
| Main, 250-ms source audio offset, default PNS | 1463/1500 s | 37/1500 s (2220 video ticks) | 1184 audio samples |
| High, aligned source audio, default PNS | 24/25 s | 1/25 s (3600 video ticks) | 1920 audio samples |
| Main, 250-ms source audio offset, PNS disabled | 1463/1500 s | 37/1500 s | 1184 audio samples |

All six alternative outputs preserve every encoded packet hash/count, packet duration,
translated PTS/DTS exactly, and decoded video frames 15–23. No discard-flag packets
were observed. These are specific FFmpeg controls, not evidence about Apple clients.

A bounded box inspector reads movie/track timescales and actual `elst` entries, rather
than inferring edits from the first reported PTS. It handles version-0/1 edit widths,
signed media times and bounded entry counts, refusing unsupported rates/layouts.

**Earliest-origin outputs:**

- AAC starts at zero with a media-time-zero edit and no skip/discard side data.
- Video has an empty edit of 17760 or 28800 movie ticks, preserving its exact offset.
- All **30720 decoded audio samples** match the fresh whole-window AAC control.
- This removes the baseline's approximately one-second empty movie interval; it does
  not make video begin at zero or remove audio preceding video.

**Video-origin outputs:**

- Video starts at zero. AAC starts at **−1184 / −1920** audio ticks.
- The audio edit has media time **1184 / 1920**; ffprobe reports matching first-packet
  `Skip Samples`, with zero discard padding. All 30 encoded AAC AUs remain present.
- FFmpeg decodes **29536 / 28800 samples**, exactly the corresponding fresh-window
  PCM slice beginning at the video boundary. This is real decoder-visible front
  omission through container signaling, not merely analytical slicing of the output.
- The sink's fragmented edit entries have zero segment duration. This observed layout
  is not proof of zero-length media, a finite end edit, or arbitrary-player behavior.

The baseline edit lists, exact track clocks, skip metadata, packet flags and PCM
comparisons are recorded in [curated results](clip-presentation-results.json).

## Decoder state and end boundaries remain explicit

Moving the movie origin and suppressing pre-video PCM does **not** reconstruct AAC
state from the continuous decoder. At the video boundary, default-PNS Main still
differs at **662 samples**, High at **690**, both with maximum s16 difference **1**.
The PNS-disabled control is exact from that boundary. These measured comparisons are
not tolerances, audibility claims, generic fixed-preroll permission or camera evidence.

The successful front edit also says nothing about a recording's tail. Main audio
still extends **23/1500 s (736 samples)** beyond video; aligned High ends with video.
No end trimming, padding removal, duration-driven cut scheduling or partial-AU encoding
was added. Packet-copy timing and decoder sample presentation are separate contracts.

## Validation and next step

The opt-in Nix integration compares all three baselines and six alternatives. Full
integrations, **215 Python tests**, and **61 Rust tests/doctests** pass. Nine focused
unit tests cover exact common translation, input independence, preserved counters and
fields, nonrepresentable origins, overflow, unknown clocks/end, keyframe refusal,
record/byte caps and bounded edit-box parsing.

Next: explicit recording end-presentation controls—whole-AU tails versus a shared
video end—without assuming that a front edit proves tail handling. Production policy
selection still needs physical-camera AAC behavior and Apple live/HSV acceptance.
Aggregate jobs, overlapping/duration-driven cuts, real transport backpressure and Rust
supervision/privacy remain open. Production and physical cameras were untouched.
