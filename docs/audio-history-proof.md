# Owned AAC history and leased history/live handoff

`scripts/audio_history.py` adds actual owned AAC wire bodies to the earlier
[metadata-only window planner](audio-window-proof.md). This remains an opt-in
Python experiment: normalized native captures are replayed through it, not a
production registry or continuously running camera/recording pipeline.

## Ownership and limits

`HistorySource` owns the admission gate and history together. Feed already
clock-admitted AMC2 frames; the existing clock normalizer/supervisor still owns
RTCP evidence, socket freshness and source generation decisions. On privacy,
source failure, cancellation or retirement, the owner must call `HistorySource.close`
before awaiting worker cleanup. Closing its private gate independently is not the
lifecycle contract.

The initial subscriber observes all original packets through active delivery leases.
For eligible AAC records, the history creates a **new immutable copy of the complete
AMC2 body**, including clocks and side metadata. It does not retain a source-frame
alias after releasing that subscriber's delivery. Copying is intentional; this is
not a zero-copy or performance result.

Defaults limit history to 64 unique bodies and 2 MiB of owned wire-body bytes, with
the planner's 1/4-second cache media horizon and 0.5-second post-normalization
publication age. The allocation is checked before making the body copy. Index
records and configuration/queue/Python overhead have separate bounds and are not
included in the owned-byte number. No process-RSS limit is claimed.

A selection pins the covering AAC AU and subsequent historical AUs, optionally
including one preceding AU for the existing diagnostic. Prefixes shared by several
handoffs reference the same owned bodies. Accounting distinguishes:

- `bytes`: unique owned bodies, whether cached, pinned, or both.
- `pinned_bytes`: unique bodies with active selection pins.
- `leased_bytes`: sum of per-consumer prefix charges; overlapping leases can count
  the same bytes more than once. Do not add these three counters together.

Cache eviction does **not** remove pinned bodies from the owned-byte/count budgets.
An insertion that would exceed those total budgets closes history and its handoffs
with overflow; ordinary live-only subscribers and the source remain independent.
There are at most four history/live handoffs by default (configurable 1–7), including
handoffs that have already released their prefixes and moved to live input.

The cache media horizon is not a bound on the span between old pinned prefixes and
the newest cached packet. Pins instead retain their original eligibility deadline
and global byte/count charges. No age is reset at selection, dequeue or phase change.

## Handoff and retirement

`open()` returns no handoff unless a current source-issued IDR admission and complete
AAC timestamp coverage exist. The source identity, exact snapshot binding and
half-open interval rules remain those of the planner. Historical/live order is:

1. Borrow the immutable historical bodies through `handoff.history`.
2. Process them and release that batch lease, promising to drop borrowed aliases.
3. Receive ordinary live delivery leases starting at the **original video IDR**,
   with original global counters and clocks. Live delivery before prefix release
   is refused. The existing target queue budgets/deadlines run during the prefix
   phase too; they are not reset by the transition.

The historical AAC sequence numbers need not be globally adjacent: pre-IDR video
packets are intentionally excluded. AAC timestamps remain contiguous, and the live
queue validates its original source sequence/ordinal/end count. No frame is silently
retimed or relabeled in the shared source; only the private finite mux IPC counters
are generated afterward.

One reusable history timer expires cached bodies and held prefixes even without new
input. Synchronous accesses/releases also check expiry when callback dispatch is
late. A held-prefix timeout detaches only that handoff. Queue failure and cancellation
of a blocked live receive release its selection and wake peer waiters on that queue.
Source-owner close synchronously retires all historical and live leases, clears owned
bodies/charges, and cancels the history timer. First terminal causes are preserved.

Finite source end seals history against new selections while existing handoffs drain.
After receiving the validated end and releasing **all** live leases, call `finish()`
to close/wake readers with `source_complete`. It refuses incomplete or in-flight input.
Receiving end alone is not recording/cloud acceptance, and source EOF/failure cannot
be promoted to successful completion. A recording-specific cut while acquisition
continues is not yet implemented.

All enforcement is cooperative Python API ownership. Escaped immutable bytes or
memoryviews cannot be revoked; zero managed bodies does not prove zero allocations,
reclaimed RSS or erased data. Consumers' own copies (including the finite diagnostic
capture below) are outside these history budgets and need their own lifecycle policy.

## Native evidence now uses actual history bodies

The four-case Main/High late-audio probe now obtains the historical prefix from its
history lease and the rest from real live-queue delivery leases. It copies these into
an explicitly finite diagnostic collector (at most 256 bodies / 2 MiB per handoff),
then feeds the native mux from those deliveries and source-issued configuration.
The retained original capture is only the equality oracle, not the source of mux
packets. Every original metadata field, payload, source sequence and end count must
match the selected source input before private counter remapping.

All eight window outputs preserve the previous exact packet clocks/hashes, original
video frames 15–29 and direct-ADTS PCM controls. The historical prefix still contains
four/five AUs; output still includes the whole leading audio. Default-PNS PCM
mismatches are not trimmed, padded or accepted under a tolerance. The presentation
and PCM conclusions from the earlier proof are unchanged.

On Darwin arm64 / Python 3.14.10 / FFmpeg 9.0.2, these finite replays peak at **11 owned
bodies**, totaling **4708–4846 bytes**. Every case closes with zero owned bytes, no
registered handoffs and no history timer. These are owned-copy counters, not memory
or sustained-throughput measurements. [Numeric observations](late-audio-results.json)
include the new `body_history` section.

## Failure coverage and remaining work

Unit cases cover actual prefix/live ordering, full source end counts, explicit drain
completion, shared pins, retention after cache eviction, independent count/byte
pressure, idle and delayed-dispatch expiry, immutable lease deadlines, owning copies,
factory guards, cancellation/multiple waiters, same-number foreign generations, and
source retirement with held historical/live data. Eight synthetic retirements require
zero registered body-wrapper owners after collection; an escaped-byte example
explicitly demonstrates the limit of revocation.

Full Nix integrations and the native handoff/mux cases, 179 Python tests, and 61 Rust
tests/doctests passed. The retired old Nix worker path was unavailable; evidence was
rerun against the newly built worker and matching Nix FFmpeg/Python binaries, not
credited from a failed invocation.

Next: paced continuous native acquisition through this owner under stalls, slow
prefix consumers, source/configuration failures and generation churn. Then specify
recording-specific cut/end and common clip presentation/edit semantics. Production
Rust integration, privacy policy across consumers' copied output, both camera models,
and Apple live/HSV acceptance remain open. No camera or production setting changed.
