# Continuous native acquisition and leased fanout proof

`scripts/native_fanout_probe.py` connects the existing experimental components while
the native worker is **still acquiring** synthetic loopback RTSP:

```
loopback RTSP -> libavformat worker -> AMC2 -> ClockNormalizer
             -> LateAdmission -> independent Delivery consumers
```

Unlike the earlier captured/replayed late-IDR proof, this driver does not accumulate
original/normalized packet lists or output clips. It retains counters, rolling hashes,
small lifecycle summaries, and weak frame/range observations. The normalizer's existing
bounded startup batch remains; afterward it releases one inspection object per packet.
An explicit dispatch yield lets subscribers run between publications, including within
the initial batch.

This is an opt-in probe, **not production ingest or a performance benchmark**. The clock
normalizer still uses hex inspection objects, and the boundary re-encodes AMC2 before
shared publication. H.264 evidence checks also make bounded copies. The fixture owns
finite generated input media and its scheduled input events; that input storage is not
an accumulated native output capture. No zero-copy, parse-once-across-all-stages, total
memory, decoder throughput, or production latency claim follows.

## Cases and admission

Each source uses one native process and exactly one loopback RTSP connection, regardless
of the number of subscribers. All cases use explicit zero-reorder Main/High video,
48 kHz mono AAC delayed 250 ms, and staggered early sender reports.

- An initial healthy consumer releases each delivery after hashing its entire normalized
  wire body, including timing/sequence metadata and payload/side data.
- Another consumer retains its first delivery but releases subsequent deliveries. Its
  0.4-second synthetic residence deadline must abort only that peer. This is deliberately
  different from merely stopping queue reads and overflowing a queue.
- A late healthy consumer requests admission after the eighth video packet and joins
  the next actual IDR, when available. Snapshot counters/clocks remain unchanged.

The publisher maintains an independent expected rolling hash/count for each healthy
consumer's admitted suffix. Equality establishes delivery of the normalized published
wire bodies, **not** independent correctness of clock normalization, complete delivery
of every fixture RTP packet, decoded frames, or AAC PCM. Those earlier packet/clock/
video-decode proofs remain separate. The holding consumer's aborted suffix is not
credited as complete delivery.

## Lifecycle propagation

Every native read is bounded by the clock normalizer's current deadline, including idle
reads. A silent open source therefore expires without waiting for another packet. EOF
from a live worker is a failure, never a successful finite recording end.

On source failure, the driver closes the admission gate and clears clock state first.
It then awaits subscriber termination **before awaiting explicit worker stop/reap
cleanup**. The process may already have exited on EOF; the ordering is not a claim that
it remains alive in every failure mode. Cancellation follows the same cleanup path,
propagates `CancelledError` to the caller, and does not masquerade as successful source
completion.

The serial driver opens the next generation only after the preceding worker and
fixture have closed. Generations have distinct source identity tokens and alternate
Main 320×180 / High 352×198 configuration. This is controlled serial restart using new
loopback endpoints—not a production registry, retry/backoff policy, simultaneous
multi-camera supervisor, or hardware reconnect acceptance test.

An in-generation replacement SPS is rejected by clock evidence before publication;
healthy subscribers receive `source_failed`, and no replacement-IDR late admission
escapes. Source freshness and subscriber residence remain independent policies.

## Completed native observations

Repository-pinned FFmpeg 9.0.2; generated local media; eight-second primary case:

| Generation / fault | Published video / AAC packets | Initial / late healthy deliveries | Outcome |
|---|---:|---:|---|
| Main, 8 s then disconnect | 120 / 364 | 484 / 430 | Live EOF → `source_failed` |
| High, silence after 0.5 s | 7 / 12 | 19 / no late IDR | SR wall freshness expiry while worker still alive |
| High, 2.2 s then disconnect | 33 / 92 | 125 / 71 | Live EOF → `source_failed` |
| Main with replacement High SPS | 15 / 39 | 54 / no late admission | Parameter-set change rejected |

Both successful late admissions began at original global sequence **54**, video PTS
**90000/90000 = 1 second**. Healthy consumer hashes/counts matched. Every holding peer
terminated with `residence_expired`; healthy peers subsequently terminated with
`source_failed`. Main's initial consumer peaked at 6,594 queued/in-flight wire bytes
(one delivery); its holding peer peaked at 8,073 in-flight bytes (two deliveries).
These are scoped managed-body counters, not RSS or general memory bounds.

Across these cases the observed peak was two registered shared frames and eight
registered byte ranges. **After every generation, both registered-owner counts were
zero**, including after forced owner-task cancellation in a fifth generation. That
cancellation waited for an actually retained delivery and asserted it was outstanding
at closure. Cancellation propagated, all subscriber tasks completed, managed bytes were
zero, the candidate timer was cleared, and the native worker was reaped.

Weak observations cover only registered publications. They do not cover all native
allocations, copied inspection objects, fixture input storage, or external aliases.
Existing cooperative lease limitations still apply: releasing a lease cannot revoke
an escaped Python frame reference or memoryview.

## Reproduction and remaining gates

The Nix check runs the same lifecycle matrix with a three-second primary case:

```sh
nix build .#ingest-worker-probe --no-link -L
# Inside the derivation's development environment, using its matching built worker:
python3 scripts/native_fanout_probe.py --worker /path/to/ingest-worker-probe --seconds 8
```

The driver limits the primary duration to 3–20 seconds. Fixture generation/send-duration
options are bounded and preserve existing default behavior. Unit checks cover the
rolling hash oracle, a missing expected delivery, independent held-peer expiry, and
fixture duration validation. Existing native mux/decode/clock suites remain enabled.

A subsequent [matched native resource comparison](native-resource-proof.md) measures
shared versus independent acquisition at 1/2/8 consumers and extends to twenty seconds.
Those scoped measurements are separate from these correctness runs and do not predict
production gains. Late AAC preroll/PCM and mux adapters, broader codec representations,
hardware cadence/budgets, both real camera models and Apple playback/recording acceptance
remain open. No camera access/configuration, production process, bridge setting, or
production ingest changed.
