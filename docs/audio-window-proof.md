# Timestamp-selected AAC start windows

The [late-AAC diagnostic](late-audio-proof.md) exposed 66.7/82 ms gaps when audio
starts with the packet-order suffix after an IDR. This next experiment selects
complete AAC access units by timestamp. It separates **packet coverage**, **fresh
versus continuous PCM**, and **what leading audio actually remains in the file**.

This is a bounded **metadata-only planner plus finite captured-oracle mux controls**.
It is not an implemented media-history buffer, live replay handoff, recording API,
production change, or Apple acceptance result.

## Planner and lifetime contract

`scripts/audio_window.py` observes an initially admitted subscriber's active delivery
leases under the fixture's single-AU-per-packet contract; it does not establish that
contract for other AAC sources. Each immutable index record contains original sequence/ordinal, exact
rational start/end times, represented AMC2 body size, payload SHA-256 and a deadline.
It retains no AAC packet body, `SharedFrame`, or payload view. The subscriber queue
has its existing separate lease/buffering limits; configuration remains source-bound.

Defaults are 64 index records, 2 MiB of **represented** wire-body sizes, a 1/4-second
media horizon and 0.5-second eligibility age. These are not measured heap/RSS limits
or managed ownership of encoded historical media. Old prefix records are explicitly
evicted, with counters; missing coverage returns no plan rather than guessing or
retiming an AU. One individually oversized AU, a receipt gap/duplicate or an AAC
clock gap/overlap fails the index closed locally. Other subscribers remain independent.
The media horizon includes progress on video as well as audio, so old audio does not
remain eligible merely because that track stopped advancing.

Age begins at the original post-normalization publication time inferred from the
lease deadline, not at dequeue or selection. Observe/plan check expiry synchronously.
`next_deadline` / `expire()` allow the owning supervisor to expire idle metadata;
**this planner creates no autonomous timer and is not wired into a live supervisor**.
Source retirement invalidates subsequent operations. Close drops records/configuration
and cancels only its own subscription. This does not establish upstream buffering or
end-to-end latency bounds.

Selection requires the exact source-issued admission for the currently observed IDR,
with matching generation/source identity and no unread publisher progress. A source
queue now weakly binds its original snapshot; reconstructing even identical snapshot
fields is not a grant. The finite late-mux adapter uses the same binding. The weak
reference does not prolong configuration ownership. These are cooperative Python
factory guards, not protection against hostile code rewriting private attributes.

An AAC interval covers the video boundary only when `start <= IDR < end`. A boundary
exactly at an AU end requires the next AU; absent future coverage is refused. The
optional `preceding=1` is an explicit diagnostic choice, not a promise about decoder
preroll. Returned tuples are immutable scalar/hash diagnostics, **not replay authority
or media leases**. Copies of those diagnostics are outside index budgets and cannot
be revoked by source retirement.

## Native finite controls

The existing four-case Main/High loopback proof now also creates two window outputs
per case: the covering AU onward, and one additional preceding AU. Plans are checked
against actual native captured payload hashes and clocks. The output is deliberately
materialized from the retained finite capture oracle, not from an implemented live
history store. Historical audio is prepended to the original late packet suffix in
source order; private sink counters are generated without changing source frames.

Every window output must have:

- A contiguous original AAC suffix, including the AU covering the video IDR.
- Exact original PTS, DTS, durations and encoded packet hashes against the full-mux
  control; no independent track zeroing or new source epoch.
- Exactly original video frames 15–29 after independent decoding.
- PCM identical to directly decoding unchanged ADTS units for the same selected range.

The common source epoch is retained. The explicit diagnostic presentation policy is
**include complete leading audio units before the video IDR**. We request no edit-list
trimming, hidden skip-sample metadata, inserted silence or dropped leading PCM to
make the file appear to start exactly at the IDR. Native mux edit-list behavior is
still checked through exact packet clocks. Apple presentation/seek behavior is not
established.

## Observations

Darwin arm64, Python 3.14.10, FFmpeg 9.0.2; synthetic AAC-LC 48 kHz mono. Video begins
at source time 1 second. Eight new window outputs pass the native controls.

| Audio timing | Historical AUs selected | First AAC index | Total AAC AUs | Audio before video, retained |
|---|---:|---:|---:|---:|
| Aligned, covering AU | 4 | 46 | 48 | 7/375 s = 896 samples |
| Aligned, plus one AU | 5 | 45 | 49 | 1/25 s = 1920 samples |
| 250 ms delayed, covering AU | 4 | 35 | 48 | 1/300 s = 160 samples |
| 250 ms delayed, plus one AU | 5 | 34 | 49 | 37/1500 s = 1184 samples |

The delayed rows apply to Main/High and the Main PNS-disabled control. Packet coverage
at the IDR is restored without pretending all audio/video start times coincide.
Each index peaked at 11 records, representing 4708–4846 bytes of AMC2 audio bodies,
**none of which it retained**. This is not a media-buffer memory measurement.

### PCM remains a different requirement

With only the covering AU, the freshly decoded audio still differs from continuous
PCM at and after the video boundary. Including one preceding AU moves the larger
startup discrepancy before the video boundary in these fixtures, but does not make
the entire output PCM exact:

| Case, one preceding AU | Differing samples from video boundary | Maximum signed-16 error there |
|---|---:|---:|
| Main aligned, default PNS | 1108 | 1 |
| Main delayed, default PNS | 1108 | 1 |
| High delayed, default PNS | 1108 | 1 |
| Main delayed, PNS disabled | 0 | 0 |

**All complete window outputs still contain fresh-decoder differences before video.**
Leading audio remains in the actual file, including that startup region. Comparisons
slice PCM at the exact integral-sample video boundary only for analysis. No difference
is hidden by a tolerance, and no audibility/conformance conclusion is drawn. General
PNS/codec behavior, Apple decoding and a suitable production presentation policy
remain open. [Numeric observations](late-audio-results.json) include both full-window
and video-boundary mismatch extents/counts.

## Coverage and next boundary

Unit coverage includes negative origins, exact half-open boundaries, absent future
coverage, exact cap equality, byte/count/media eviction, video-only progress, original
publication age, supervisor-driven idle expiry, source retirement, foreign admissions,
released/duplicate/skipped receipts, gaps/overlaps, immutable plans, reconstructed
snapshots and lack of retained shared audio frames. Full Nix integrations (including
all eight outputs and existing lifecycle/ownership checks), 170 Python tests and
61 Rust tests/doctests passed.

Next work is an actual source-owned bounded AAC body history with charged selection
leases and a generation-safe historical/live handoff. It must preserve these interval
rules while proving cancellation, idle expiry, shared-body accounting and privacy
invalidation. The finite oracle is not that implementation. Real recording cut/end
and common clip presentation/edit semantics must also be explicit; neither packet
coverage nor these PCM controls authorize production or HomeKit/HSV rollout.
