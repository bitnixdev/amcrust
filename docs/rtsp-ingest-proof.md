# Loopback RTSP ingest and generation proof

Status: the synthetic RTSP gate is complete for the cases below. Production
amcrust still uses its original readers. No real cameras, credentials, HomeKit
clients or deployed services are accessed by this experiment.

## Reproduce

```sh
nix build .#ingest-worker-probe --no-link -L
python3 -B -m unittest discover -s scripts -p 'test_*.py' -v
```

The package's check phase now runs both the original packet-preservation proof and
`scripts/rtsp_ingest_probe.py`. All tool dependencies remain repository-managed.
Tests need permission to bind ephemeral **127.0.0.1** TCP ports. There is no public
listener or external network access in the generated fixture. Results may be cached
by Nix, as with any successful package build.

## Fixture and scope

`scripts/rtsp_fixture.py` implements only the RTSP subset needed for this proof:

- OPTIONS, DESCRIBE, SETUP, PLAY, GET_PARAMETER and TEARDOWN.
- Digest MD5 authentication without qop, using fixed synthetic credentials, realm
  and nonce. It verifies the username, method, URI and response, and has an explicit
  authentication-rejection mode. This is not a production authentication service.
- H.264 (payload type 96, 90 kHz clock), including FU-A fragmentation; AAC-LC mono
  48 kHz (type 97) using MPEG4-GENERIC AU headers. Both use RTP-over-RTSP TCP channels.
- SDP codec configuration and track setup. Media is generated locally with FFmpeg:
  320×180 or 640×360 at 15 fps, IDR every second, no B-frames, plus a sine-wave track.
- Deliberate disconnection, live-socket media stall, PLAY with no media, and a
  backward video RTP timestamp jump. This is not a general RTSP server or proxy.

The C worker's default invocation still allows only local files. The new form is:

```text
ingest-worker-probe --rtsp-fixture PORT GENERATION
```

It constructs its own numeric-loopback URL and synthetic fixture credentials;
there is no arbitrary camera-URL option. It requests TCP transport and a native
RTSP read timeout, suppresses URL-bearing library diagnostics, and reports any
live-source EOF/read failure as failure rather than a successful finite clip.
The generated server does not redirect or forward connections; this is a trusted
loopback fixture, not a security sandbox for a malicious RTSP endpoint.

## Observed assertions

Built/run on macOS arm64 with the flake's FFmpeg 9.0.2:

| Scenario | Assertion |
| --- | --- |
| Authenticated stream then disconnect | 33 video and 104 AAC packets; source failure rather than successful `end` |
| Reopen the same endpoint with 640×360 instead of 320×180 | New generation, different SPS/PPS/extradata, correct new width |
| Media stops but RTSP socket stays open | Native read failure or supervisor watchdog terminates the attempt within the fixture deadline |
| Video RTP clock moves backward | Supervisor observes the rollback and invalidates the source |
| PLAY succeeds but no media arrives | No packets admitted; bounded failure |
| Authentication rejected | No PLAY and no media; bounded failure |
| Viewer joins after seven video packets | Interframes/audio are withheld until a valid IDR; current codec descriptors precede admitted media |
| Old packet offered after reopen | Rejected as a stale generation |

AAC packet hashes match the original generated AAC access units, with the expected
48 kHz time base. The late viewer's admitted H.264 packets, prefixed with that
generation's codec configuration, are decoded by FFmpeg with errors fatal at both
resolutions. This proves a decoder can start on the admitted video; it is **not**
a HomeKit RTP/SRTP playback, HSV muxing, or synchronized A/V output proof.

Timing checks are fault-test deadlines, not performance benchmarks. The worker has
a two-second native RTSP timeout, while the supervisor applies a four-second
initial-frame / 2.5-second subsequent-frame watchdog. FFmpeg may perform multiple
reads during discovery, so the native option alone is not an end-to-end deadline.
Each complete test run also has a 60-second outer bound.

## Generation and admission rules

`SourceSupervisor` explicitly reopens attempts; it does not yet implement an
unattended retry/backoff loop or production privacy/lease handling.

- Reject overlapping attempts. Increment the supervisor-owned generation for each
  new worker, and require that value on every IPC frame.
- Clear/invalidate consumer queues and codec state on termination. Reap the old
  worker before admitting the next attempt.
- Never accept a packet or descriptor from an earlier generation.
- `LiveAdmission` supports the Annex-B H.264 emitted by this RTSP fixture. It needs
  both SPS and PPS in the current descriptor or the admitted access unit, an actual
  IDR NAL, and the keyframe flag. The flag alone is insufficient.
- Do not remember parameter-set-only packets discarded while waiting for an IDR;
  otherwise a decoder would miss configuration that the admission gate assumed it
  had received. Old-generation parameters are never reused.
- Overflow detaches and clears the viewer; it does not silently discard arbitrary
  encoded packets while pretending the decoder remains synchronized.

Rollback detection uses DTS when present, otherwise PTS. That fallback is valid
only for these **known no-B-frame fixtures**; a production source must not mistake
legitimate reordered PTS for a clock fault. Missing-timestamp handling remains
protocol-unit coverage, not a simulated RTP scenario (RTP headers always carry a
timestamp). Normal B-frame packet preservation is covered by the local-file proof.

Queue byte/count bounds and slow-consumer isolation are covered by the existing
fanout suite and admission tests. The RTSP integration drains one healthy late
viewer; it does not claim concurrent HSV/live throughput or media-time budgets.

## Cleanup correction

All probe subprocess cleanup now kills if necessary, drains stdout/stderr using
`communicate()`, and reaps. Awaiting `wait()` alone can deadlock with asyncio's pipe
transport when stdout has reached the reader's high-water mark. A regression test
fills/pauses a small stdout buffer and checks cleanup completes after killing the
writer. This changes only the research harnesses, not production process handling.

## Still open

1. [Finite-file A/V output reconstruction](av-output-proof.md) now preserves tested
   packet timing/payloads and prevents mixed init/fragments. Terminal AAC padding
   remains explicit. [Clocked non-reordering RTSP output](rtsp-clock-proof.md) now succeeds
   using early sender reports and bounded startup repair. Unclocked or ambiguous
   reordered timestamp input is still rejected.
2. Replace the inspectable hex-JSON prototype with bounded binary IPC and implement
   media-duration queue budgets before making performance claims.
3. Broader RTSP interoperability: qop variants, RTCP sender-report clock mapping,
   in-session codec changes, B-frame/discontinuous clocks, and actual camera quirks.
   This fixture changes codec configuration on reconnect, not within one session.
4. Production Rust integration, continuous supervision/backoff, subscriber terminal
   notifications, privacy/lease ownership, and real multi-consumer load tests.
5. Both physical camera models plus Apple live/recording acceptance and matched
   before/after CPU/RSS, connection-count, startup and recovery measurements.
