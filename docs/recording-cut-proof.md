# Recording-local cut while acquisition continues

`scripts/recording_cut.py` adds `RecordingCutInput`, a bounded diagnostic builder
consuming an actual source-issued `Handoff`. It reuses the finite adapter's private
IPC budget/counter handling but **does not require, manufacture or publish a shared
source end**. `scripts/recording_cut_probe.py` exercises it during paced native RTSP
acquisition, not capture replay. This remains outside production.

## Boundary and cancellation contract

1. Copy the charged historical AAC prefix under its actual lease, checking source
   generation, sequence, ordinal position, timestamps and payload hash. Release that
   prefix before receiving live data.
2. Accept only this subscriber's active delivery receipts, in original source
   ordinal/sequence order, starting at its admitted IDR. The sink-local packet counter
   includes the prefix; source counters and clocks remain unchanged.
3. `prepare_cut()` requires live data for all declared tracks and no outstanding
   receipt. It freezes the boundary at the **last fully consumed source packet**.
   This is a packet-order boundary, not an exact requested wall time or shared A/V
   sample endpoint. No further `feed()` is allowed.
4. `commit_cut()` requires a still-active grant, no source finite end, no outstanding
   receipt and a valid complete private protocol. It emits a **private sink end** with
   the selected packet count. Newer queued source media is deliberately excluded;
   closing just this handoff uses `cancelled`, never shared `source_complete`.
5. Subscriber cancellation, residence expiry, owner retirement or validated source
   end **before transfer** denies commit and clears the builder's copies. Preparing
   does not override those causes. The owner must call abort/finalization on teardown;
   the builder does not run an autonomous cleanup task for its independent copies.
6. A successful commit transfers ordinary owned copies to the caller. Later source
   failure does not revoke those copies. This is a deliberate diagnostic ownership
   boundary, **not a production privacy permission or recording-delivery outcome**.
   Copied-input/output cancellation during muxing/HDS still needs its own lifecycle.

Defaults are 256 total selected packets and 2 MiB of retained representation, charged
as the larger of encoded AMC2 and inspection JSON for each record, including headers
and the final private end. These bounds do not cover arbitrary caller-retained results
or establish an RSS bound. A bad receipt/budget/state closes only the recording's
handoff. The historical body store and ordinary live subscribers remain independent.

## Explicit experimental presentation policy

The current baseline retains **whole leading AAC AUs and the original common source
epoch**, with no independent track zeroing, invented timestamp, PCM trimming, silence
insertion or edit intended to hide decoder differences. Mux settings remain the
existing `delay_moov`/edit-list packet-copy controls. The video begins at source 1 s;
this is not a finalized zero-based Apple clip presentation policy. Leading audio and
any audio tail beyond video are reported rather than silently removed.

The experiment cuts after consuming video frame 23. Nine video frames (15–23) and 30
AAC AUs are included. Commit occurs only **after another source packet has arrived**,
proving that later queued media is excluded rather than redefining the cut to the
latest publisher position. An independent bounded source hash oracle checks all
selected wire fields after restoring only their original sequence numbers.

## Native results

Darwin arm64 / Python 3.14.10 / FFmpeg 9.0.2. Each case has one worker, one connection
and one PLAY. Both successful cuts happen while the worker and source epoch remain
active; each source subsequently publishes **99 additional packets**.

| Case | Published video / AAC | Ordinary admitted deliveries | Cut sequence | Result |
|---|---:|---:|---:|---|
| Main 320×180, AAC offset 250 ms | 48 / 139 | 187, starting sequence 0 | 87 | private 39-packet clip |
| High 352×198, aligned AAC | 48 / 150 | 194, starting sequence 4 | 98 | private 39-packet clip |
| Main, subscriber cancels prepared cut | 48 / 139 | 187 | 87 | commit denied; live continues |
| Main, changed SPS at frame 30 after preparation | 30 / 86 | 116 | 87 | source retirement denies commit |

The aligned source publishes four AAC packets before its initial live-only IDR
admission; they are not falsely counted as ordinary-consumer deliveries. All admitted
ordinary suffix counts and hashes match. Source EOF remains `source_failed`, not
recording completion. Both refusal cases retain no private builder frames/bytes;
no mux is invoked for them. Workers are reaped and source history bytes/timer cleared.

Successful private inputs are muxed only after this acquisition run ends, making the
ownership distinction explicit: input cut/transfer is tested during acquisition,
**concurrent muxing and streaming output delivery are not tested here**.

For both clips:

- Original packet PTS, DTS and duration are exact through mux; AAC packet hashes match.
- Decoded video is exactly original fixture frames 15–23, not merely nine packets.
- PCM is exactly the fresh direct-ADTS selection: **30720 samples** from 30 whole AUs.
- Main audio leads video by **37/1500 s** (1184 samples) and ends **23/1500 s** after video.
- High audio leads by **1/25 s** (1920 samples), ending with the video.

Continuous decoding remains different with the default PNS encoder. The whole Main
window differs at 1682 samples (maximum s16 error 4623); High at 1729 (maximum 5013).
From the video boundary, Main still differs at **662 samples**, High at **690**, each
with maximum error **1**. Those are analytical comparisons, not an error tolerance,
audibility statement, generic one-AU preroll guarantee or actual output trimming.
See [curated results](recording-cut-results.json) and the earlier
[AAC decoder-state controls](late-audio-proof.md).

## Validation and remaining work

The opt-in Nix integration now runs all four cases. Full integrations, **193 Python
tests**, and **61 Rust tests/doctests** pass. Eight focused unit tests cover local
cut/continued source operation, original counter isolation, newer queued data,
retirement/cancellation/expiry precedence, finite-source-end refusal, foreign or
outstanding receipts, invalid transitions and header/prefix/live/end budgets.

The subsequent [owned output lifecycle](recording-output-proof.md) now exercises
asynchronous mux/delivery, cancellation and bounded output files while acquisition
continues. Next: common-origin presentation/edit alternatives, kept separate from
this exact-clock baseline and its remaining decoder-state differences. Duration-driven cut scheduling, overlapping recording cuts,
long clips, output backpressure, general PCM-exact start/end behavior, production Rust
supervision/privacy and physical-camera/Apple live/HSV acceptance remain open. No
production process or camera setting was changed.
