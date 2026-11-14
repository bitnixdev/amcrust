# Admitted-subscriber ownership and residence proof

This extends the [late-IDR prototype](shared-late-admission-proof.md), not production
streaming. It adds **cooperative delivery ownership** and receiver-monotonic residence
limits, alongside the existing independent queued byte/count/media limits.

## Delivery contract

An admitted queue's `receive()` now returns a `Delivery` lease:

```python
with await admission.queue.receive() as delivery:
    await process(delivery.frame)
    # Drop frame/view aliases before leaving this context.
```

The queue charges the immutable wire-body bytes and one outstanding frame until
explicit `release()` or context exit. Release is idempotent; context exit also runs
on exceptions/cancellation. A consumer that drops the lease without releasing it
remains charged until its deadline or generation closure. Deadline/size/identifier
properties are read-only.

The synthetic defaults are **two in-flight frames**, **16 MiB in-flight wire-body
bytes**, and **two seconds residence**. Equality with a byte/count cap is allowed;
a further delivery exceeding either cap aborts that subscriber with `delivery_limit`.
Both queued data and all managed leases are retired, waiters wake, and other
subscribers continue. The existing 0.5-second media-envelope budget still applies to
**queued** media, not to the combined queued/in-flight timeline.

Residence is one deadline from **post-normalization source `feed()` to release**.
Joining an existing candidate or moving a frame from queue to delivery does not reset
its age. This does not include upstream RTSP reads, clock-normalizer startup retention,
codec work before this stage, or downstream work after release. It is not an
end-to-end latency bound.

Idle timers enforce both queued and in-flight expiry without source packets or
consumer polling. Expiry aborts with `residence_expired`. Offer, receive, frame access
and release also check the deadline so delayed timer dispatch cannot renew an overdue
lease. A stalled event loop cannot execute a timer on time; access is refused when it
resumes. Close preserves the first terminal cause, clears managed data and cancels
its timer, whether the reason is failure, reconnect, cancellation or finite completion.

Each queue reuses one pending wakeup when deadlines move later or the queue becomes
empty. An earlier harmless wakeup rechecks/rearms instead of allocating a new timer
for every packet. Candidate retention uses the same strategy: the candidate expires
on the next source frame or its residence deadline, including when no subscribers
exist. A replacement IDR gets its own original feed-time deadline, not the old IDR's
deadline. Closing the source cancels the candidate timer.

These limits are **not production recommendations** and do not replace supervisor
idle/RTCP-freshness deadlines. Candidate presence never grants sender-clock freshness.
All operations belong to one running asyncio event loop.

## What the limits cannot guarantee

A lease only controls references managed by this API. Python callers can copy a frame
reference or obtain a memoryview, release the lease, and keep that alias alive. A
regression explicitly demonstrates that such an escaped view remains readable after
closure even though managed counters are zero. Retirement prevents further lease
access; it cannot revoke bytes already handed out.

Thus these are not total-RSS bounds, a hostile-client sandbox, a privacy erasure
mechanism, or proof of reclaimed backing storage. Codec snapshots and source-owned
configuration have separate lifetimes. Consumers must honor release semantics, and
production transports must independently stop delivery/encryption on lease retirement.

## Generation-churn observations

`scripts/admission_churn_probe.py` alternates Main/High configurations across unique
generations. Each generation has 30 synthetic video packets, one initial consumer,
one consumer retaining its first lease while processing subsequent packets, and one
late consumer joining the next actual IDR at sequence 15 / video PTS 90000. It checks
all counts and payload hashes, global snapshot positions, and original configuration.

The retained first delivery stays charged after its queue becomes empty. Retirement
cycles through `generation_changed`, `source_failed`, `cancelled`, and validated finite
`source_complete`. All three blocked receivers must wake with the corresponding
reason; leases become inaccessible and managed counters/timers clear. Weak tracking
requires **zero registered SharedFrame/ByteRange owners after every generation**, not
only at the end. This is a check of registered owners, not all Python allocations.

Completed local macOS/Python 3.9.6 observations:

| Generations | Payload | Delivered packets (initial / holding / late) | Peak in-flight bodies per peer | Peak traced allocations |
|---:|---:|---:|---:|---:|
| 500 | 4 KiB | 15,000 / 15,000 / 7,500 | 2 frames / 8,525 bytes | 194,806 bytes |
| 32 | 256 KiB | 960 / 960 / 480 | 2 frames / 524,623 bytes | 2,248,711 bytes |

Both runs ended with zero registered frame/range owners. Final traced allocations were
127,257 and 116,916 bytes respectively, including reporting/runtime state. Sampled
traced totals varied; no flat-heap or general leak-freedom claim is made. An attempted
500-generation large-payload run was stopped by the enclosing command's timeout and
is **not credited**; the completed large-payload observation is only 32 generations.

Payloads are opaque transport stubs with supported slice prefixes, **not decodable
AVC**. The harness includes fixture hex generation, AMC2 encoding, evidence-parser
copies, hashing, tracing and garbage collection. These measurements are not native
acquisition throughput, current/reclaimed RSS, real latency, or production/HomeKit
performance evidence. Existing native Main/High late-video decode checks remain
separate and now use explicit delivery leases.

## Coverage and remaining gates

Focused cases cover exact/cumulative byte caps, count caps, multiple waiting receivers,
idle queued/held expiry, candidate expiry, no age reset, delayed timer dispatch,
read-only lease fields, timer reuse/rearming, cancellation, generation retirement,
escaped aliases and healthy-peer isolation. Nix also runs a 32-generation churn smoke.

Next: continuous native acquisition/normalized fanout exercising these ownership rules
without accumulating full captures, including live expiry/reconnect propagation. Late
AAC preroll/PCM and mux adapters, broader codec contracts, both camera models and Apple
acceptance remain open. Nothing here replaces production ingest or changes cameras.
