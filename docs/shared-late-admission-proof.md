# Shared late-IDR admission proof

The shared-metadata path now has an experimental, source-issued late-admission
bootstrap. This closes the previous complete-generation-only restriction for a
**deliberately narrow subset**, not for arbitrary H.264 sources or production clients.

## Contract

`LateAdmission` consumes frames **after common-clock admission**. It uses the existing
`SharedSource` factory and accepts one H.264 video track plus optional AAC. Video must
satisfy the existing `ReorderEvidence` contract: Annex-B, one matching SPS/PPS,
progressive 8-bit 4:2:0 Main/High, supported POC, explicit zero-reorder VUI, and zero
`video_delay`. Baseline, AVCC, multiple parameter sets, B-frame streams, unknown clocks
and other configurations grant no admission permission in this new path.

Every packet must have known PTS/DTS, positive duration, equal PTS/DTS and increasing
per-track DTS. Every video packet is checked against the announced parameter sets
and supported slice-header prefixes—even before any viewer joins. Parameter changes,
unknown PPS references and mixed IDR/non-IDR access units retire the generation.
These are bounded evidence checks, not a complete AVC bitstream validator.

Admission requires **both** a keyframe flag and actual IDR VCL NALs referencing the
current PPS. A keyframe flag on a P/I non-IDR picture cannot grant admission. No
previous IDR or arbitrary encoded suffix is replayed.

The packet-driven `join()` operation returns `None` until the current source frame
is eligible. At that point it creates:

- An immutable snapshot of the original hello, stream descriptors, generation,
  source-instance identity, first global ordinal/sequence and video PTS.
- A bounded subscriber queue whose cursor is bootstrapped by the source factory,
  not by caller-selected counters, with that exact IDR already enqueued.

The snapshot contains codec configuration but **no retained media packet**. The gate
retains at most the current candidate IDR, which expires at the next source frame.
There is no historical GOP buffer or timestamp re-zeroing. Later packets retain
original payloads, metadata, global sequence/ordinal and shared A/V time bases.
The original global end count is checked using the admitted starting sequence.

Up to eight subscribers may be active. A slow queue detaches on existing byte/count/
media limits without stopping healthy peers. Retired queues are pruned; successful
admission does not reuse a detached queue. Live failure, clock freshness expiry,
reconnect or cancellation must close the gate, clearing its configuration/candidate
and waking receivers with the corresponding terminal reason. A finite `end` prevents
new admission but permits already queued data to drain before explicit closure.

The caller/supervisor still owns idle and clock-freshness timers. An unchanged current
candidate does not establish freshness during a silent socket; the caller must retire
the gate on expiry. Previously delivered snapshots/frames remain readable but cannot
reopen a closed gate. These are cooperative in-process rules, not authentication
against hostile Python code.

## Native fixture evidence

The Nix RTSP clock proof replays its already-normalized **actual worker captures**
through this gate for Main and High, with 250 ms delayed AAC and staggered reports.
One viewer joins at startup. Another starts requesting admission after the eighth
video frame, after the initial IDR has passed, and waits for the next actual IDR.

Both profiles produce:

- Late admission at original video **PTS 90000/90000 = 1 second**, global packet
  sequence **54**, with no source-clock or packet renumbering.
- **15 late video frames** and **44 subsequent AAC packets**, with the exact shared
  frame objects/wire bytes delivered while the initial viewer receives all packets.
- Independent video decode from the snapshot SPS/PPS plus admitted H.264 packets,
  matching source frames **15–29** exactly and in order.
- Source video timestamps remain `90000, 96000, …, 174000`; the finite source end count
  remains valid for both viewers.

Audio packets retain their original clocks and bytes, but this does **not** establish
late AAC PCM equality, preroll handling or synchronized Apple playback. Admission
skips all earlier packets in global demux order; it does not buffer earlier audio
around the IDR or hide an initial audio gap. Existing whole-clip A/V mux/decode proofs
remain separate from this late-viewer video proof.

## Regression coverage and remaining work

Unit cases cover false keyframe flags, missing flags, stale-IDR refusal, immutable
snapshots, in-generation SPS replacement before/after admission, PPS mismatch,
mixed VCL types, unsupported configurations/clocks, independent slow-peer overflow,
subscriber caps, live-failure wakeups, changed-generation snapshots, and foreign
source identities. The existing malformed-wire, clock, queue and native mux suites
remain in place.

This is a finite replay of captured RTSP media, not concurrent camera/Apple admission
or a new production subscription API. Source video inspection currently makes bounded
byte copies for the existing evidence parser; no throughput improvement is claimed.
Remaining gates include residence/in-flight policies, generation-churn load, late-audio
semantics, broader codec representations, native continuous integration and both real
camera models plus HomeKit acceptance. Production paths and cameras are unchanged.
