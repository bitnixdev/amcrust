# Synthetic sustained fanout and ownership measurements

This is an **instrumented, accelerated transport workload**, not a camera, codec,
RTSP, HomeKit or production benchmark. It exercises the shared AMC2 source/cursors
and byte/count/media-duration queues without inspection JSON or payload hex in the
measured path. Payloads and codec configuration are deliberately opaque stubs tagged
as H.264; they are not decodable AVC. Actual codec/clock/mux correctness remains
covered by the separate native fixture proofs.

## Reproduce

Run each case in a fresh process, sequentially rather than concurrently:

```sh
python3 -B scripts/fanout_load_probe.py --packets 10000 --subscribers 1
python3 -B scripts/fanout_load_probe.py --packets 10000 --subscribers 2
python3 -B scripts/fanout_load_probe.py --packets 10000 --subscribers 8
python3 -B scripts/fanout_load_probe.py --packets 100000 --subscribers 8
python3 -B scripts/fanout_load_probe.py --packets 10000 --subscribers 8 --payload-bytes 262144
```

Defaults: 16 KiB payloads, four packets per producer burst, 0.5 s media-span budget,
32 queued frames, and a byte cap of `max(1 MiB, 8 * payload_bytes + 64 KiB)` per queue.
The workload is bounded to 64–1,000,000 packets, 1–8 healthy subscribers, 256 B–256 KiB
payloads, and bursts of 1–4. It additionally creates one permanently stalled subscriber.
The Nix check phase runs a 2,000-packet/four-subscriber smoke case; unit tests use
smaller workloads and check cancellation/tracer ownership.

## What is exercised

- One source parse per frame; the same immutable metadata and backing bytes fan out.
- Healthy subscribers independently check sequence/end state and hash **every**
  payload. They discard processed frames/metadata/views before awaiting more data.
- The stalled peer detaches on its media-duration cap without stopping healthy peers.
- One consumer intentionally retains its first delivered view until the midpoint.
  Samples observe that backing body even when all queue byte counters are zero.
- After release, drained checkpoints retain only the source's one codec-configuration
  byte range. After complete queue/source/consumer teardown, all tracked published
  frames, ranges and views disappear.
- Weak ownership trackers do not keep their targets alive. At most 16 sample records
  are stored; there is no packet capture or accumulated packet-summary list.

The probe registers published frames, their byte ranges and consumer-created views
with weak references, and deduplicates their backing bodies by identity. These counts
cover **registered publication owners**, not arbitrary raw-byte aliases or all Python
objects. Weak-reference support adds no strong references to the packet types.

## Observed results

Sequential fresh-process runs on macOS arm64, Python **3.9.6**, using the commands
above. Numbers are single-run observations, not statistical performance estimates.

| Packets | Healthy peers | Payload | CPU seconds | Wall seconds | Peak traced allocation | Process peak RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 1 | 16 KiB | 1.240 | 1.263 | 179.8 KiB | 24.88 MiB |
| 10,000 | 2 | 16 KiB | 1.682 | 1.705 | 183.1 KiB | 24.97 MiB |
| 10,000 | 8 | 16 KiB | 4.485 | 4.549 | 205.9 KiB | 25.03 MiB |
| 100,000 | 8 | 16 KiB | 42.825 | 43.080 | 205.9 KiB | 25.02 MiB |
| 10,000 | 8 | 256 KiB | 12.388 | 12.414 | 2,125.8 KiB | 29.02 MiB |

All cases delivered every packet to every healthy subscriber, parsed exactly
`packets + 3` source frames (hello, one descriptor, packets, end), isolated the
stalled peer, and finished with **zero tracked publication owners/backing bodies**.
The last-quarter spread of traced current allocations was 1,888 bytes in these runs;
the bounded sample/result records themselves occupy memory. The harness has a coarse
2 MiB late-spread failure tolerance, not a production memory budget or leak theorem.

In the 100,000-packet run:

- Healthy queue peaks were 66,256 wire bytes and `4/15` media seconds each.
- The stalled queue peaked at 115,854 wire bytes and `7/15` media seconds, then
  detached on the next packet rather than exceeding its 0.5 s media budget.
- An early drained checkpoint showed zero queued bytes but two backing bodies:
  292 bytes of codec configuration and the intentionally retained first delivery.
- Late drained checkpoints showed only the 292-byte codec body; complete teardown
  removed that owner too. Final traced allocations were 81,915 bytes, not zero:
  measurements, queue objects and harness/runtime state still exist at reporting time.

The tenfold packet-count increase did not increase observed traced peak or leave
tracked publications behind. That is bounded-workload evidence under this release
policy, **not** a guarantee for arbitrary consumers or a long-running native bridge.

## Measurement boundaries and remaining gates

- CPU/wall time includes Python envelope generation, source validation, queue work,
  per-consumer SHA-256, weak tracking, `tracemalloc` and checkpoint garbage collection.
  It excludes initial fixture/empty-queue construction and final JSON printing.
  This is not a comparison with the previous implementation or a throughput claim.
- Traced allocations cover the measured Python interval, not total native allocations.
  The fixed input payload and empty harness structures are created before tracing.
  `ru_maxrss` is the process-lifetime high-water mark observed at reporting time,
  including interpreter startup and instrumentation—not current RSS or reclaimed RSS.
- Queue wire-byte totals are per-subscriber logical budgets; summing them overcounts
  shared physical bodies. Metadata overhead and delivered/in-flight owners are outside
  those counters. A consumer retaining deliveries indefinitely can still exhaust memory.
- The producer is unpaced. Media timestamps advance at 15 packets/s independently of
  wall time; neither media duration nor accelerated wall time measures camera latency.
- This covers one source generation with complete-generation consumers. Generation
  churn, shared late-IDR admission, residency/in-flight limits, native acquisition,
  decoding, packetization/encryption and real multi-camera load remain separate gates.
- No camera configuration, credentials, production processes or bridge settings change.
