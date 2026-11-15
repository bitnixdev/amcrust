# Paced native AAC history lifecycle

`scripts/native_history_probe.py` connects actual loopback native acquisition to
`ClockNormalizer` → `HistorySource` → asynchronous history/live consumers. It does
not first accumulate and replay the native output. This extends the
[owned-history replay proof](audio-history-proof.md), not production ingest.

## Bounded observations and consumers

There is one native worker and one authenticated RTSP connection per generation.
An initial healthy live-only peer receives the published stream. At the second IDR,
a healthy history peer and a deliberately holding history peer select one preceding
AAC AU plus the covering window: original source sequences **48, 49, 51, 52, 53**.
The live part begins at original **sequence 54 / video PTS 90000**, without rebasing
source counters or clocks.

The independent prefix oracle keeps only the latest 64 `(sequence, SHA-256)` pairs
from original published AAC wire bodies, never their payloads. History consumers hash
actual borrowed bodies and then actual live deliveries. Healthy history peers compare
rolling per-frame hashes and counts against that oracle plus subsequent source
publications. Missing/corrupt expected prefix hashes cannot silently pass. The older
live-only peer keeps its original rolling wire hash/count check.

The holding peer hashes its five historical bodies, keeps the batch lease charged,
and does not drain its live queue. Its history lease must expire locally while healthy
consumers continue. `Handoff.wait_closed()` now lets held-prefix consumers observe
owner/lease retirement without polling or requiring a new source packet. It is a
closure notification, not a media delivery or recording acknowledgment.

Synthetic history age is **0.4 seconds** from original post-normalization publication;
live queue residence retains its two-second default and media budget its half-second
default. Prefix age is therefore exercised separately from live queue residence.
Cache defaults remain 64 bodies, 2 MiB, 1/4-second media horizon and four handoffs.
These are experimental budgets, not camera cadence/latency recommendations.

## Fault matrix

The fixture now accepts a bounded parameter-change IDR position (default remains
frame 15). This probe changes SPS at **frame 30**, after healthy history/live handoff
has already occurred. Main uses 320×180; High uses 352×198.

Recorded eight-second primary run, Darwin arm64 / Python 3.14.10 / FFmpeg 9.0.2:

| Generation | Scenario | Published video / AAC | Healthy early / history deliveries | Holding-prefix result |
|---|---|---:|---:|---|
| 1 | Main, 8-second disconnect | 120 / 364 | 484 / 435 | 5 prefix bodies, `residence_expired` |
| 2 | High, clock/data silent from 0.5 s | 7 / 12 | 19 / no admission | no history admission |
| 3 | High, 2.2-second disconnect | 33 / 92 | 125 / 76 | 5 prefix bodies, `residence_expired` |
| 4 | High, 2.2-second stall, socket left open | 32 / 92 | 124 / 75 | 5 prefix bodies, `residence_expired` |
| 5 | Main, replacement High SPS at frame 30 | 30 / 86 | 116 / 67 | 5 prefix bodies, `residence_expired` |
| 6 | Main, early disconnect at 1.2 s | 18 / 45 | 63 / 14 | still-held prefix retired as `source_failed` |
| 7 | Owner cancellation after a consumer confirms holding | not a delivery-completeness case | cleanup assertions only | still-held prefix retired as `cancelled` |

All normal-fault healthy counts and hashes match. The history count includes five
historical AAC bodies plus the original live suffix; the holding count is not a claim
that queued live media was consumed. Both idle cases expire cached bodies to zero
**before** clock freshness failure, while the worker has not exited. The changed SPS
is refused before publication. Live EOF is failure, never fabricated finite completion.

The cancellation signal comes from the consumer after it actually borrows/hashes the
prefix and retains its lease. The owner asserts that prefix is still held at close;
cancellation propagates rather than being swallowed. This case establishes cleanup,
not lossless delivery during cancellation.

## Retirement and resource observations

Source/history retirement and clock-state clearing happen before explicit worker
cleanup. Receivers are woken, peer tasks finish, every worker is stopped/reaped, and
history, candidate and subscriber timers are cleared. Cleanup still reaches worker
stop if a retirement assertion fails, source close raises, or peer cleanup is cancelled.

Each serial restart has a distinct source identity and the newly announced codec
configuration. After every generation and cancellation, weak tracking finds zero
registered shared frames, byte ranges and history-body wrappers. This is serial
lifecycle churn, not a production retry/backoff registry.

The paced primary reaches **16 owned history bodies / 6769 bytes**, versus 11 bodies
in the immediate-release replay: held selections remain charged while newer cache
entries arrive. The holder's live queue also grows separately; its buffered frames
are not part of that history-byte counter. Peaks observed by weak tracking are
19 shared frames, 41 byte ranges and 16 body wrappers.

A second complete matrix with a **20-second primary** passes too: **300 video / 926
AAC**, **1226 early / 1177 history deliveries**, with the same 16-body / 6769-byte
history peak. All generations close with zero managed history bytes and zero
registered owners after collection. [Curated observations](native-history-results.json)
include the eight-second matrix and the longer primary plus its fault-run summaries.

These counters do not prove zero allocations, reclaimed RSS or erased escaped aliases.
The fixture retains finite generated source media/events; clock normalization still
uses bounded inspection conversion/re-encoding. No accumulated native-output capture
or clip is kept by this probe, but it is not a zero-copy, throughput, end-to-end latency,
long soak/leak, decoder, or Apple acceptance result. The earlier finite mux/PCM proofs
remain separate.

## Validation and next step

The opt-in Nix check runs the complete matrix with a three-second primary. The full
integration suite, **185 Python tests**, and **61 Rust tests/doctests** pass. New unit
coverage checks history/live hashing, incorrect/missing source oracles, multiple idle
and retirement waiters, parameter-change bounds, and worker cleanup despite retirement
or cleanup failure.

Next: recording-specific cut/end while acquisition remains active, with explicit
common clip presentation/edit semantics and cancellation precedence. Do not use source
EOF or receipt of a packet as proof of a completed recording. Production Rust registry,
consumer-copy privacy policy, broader codec/clock contracts, both physical cameras and
Apple live/HSV acceptance remain open. No camera or production setting changed.
