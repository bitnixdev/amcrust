# amcrust

Bridges a single Amcrest camera to HomeKit as a native camera/video accessory,
written in Rust. Run **one instance per camera** — each instance is its own
standalone HomeKit accessory with independent pairing state, so one camera
failing or being reconfigured never affects the others.

Supported/tested cameras: Amcrest `IP8M-2696E-AI` and `IP8M-2796E-AI`.

## Project direction and documentation

**Live video, live audio and HSV recording now use Rust-owned shared acquisition.**
There is one libavformat reader per active encoder, with independent HomeKit transports.
The old media FFmpeg subprocesses, RTP relays and timestamp filters are removed; there
is no project C worker, MediaMTX dependency or selectable legacy-reader fallback.
This completes the production media-path cutover, not deployment or HSV Apple-client
acceptance. Snapshot JPEG scaling still uses the FFmpeg executable.

- [Architecture](docs/architecture.md): current versus intended data flow and decisions.
- [TODO](docs/todo.md): the single ordered backlog, including every C replacement/removal
  and the authorized real-device testing workflow.
- [Evidence](docs/evidence.md): consolidated completed findings and unresolved limits;
  historical experiment success is not production or Apple-client acceptance.
- [HDS wire reference](docs/hds-wire-format.md): transport/protocol reference, not a roadmap.

## What the current application does

- **Live video in the Home app**: HAP RTP stream management with SRTP. Amcrust
  advertises and routes two native camera encoders without video transcoding:
  the HSV-controlled main stream for 1080p and Sub Stream 2 at 720p/15 fps/2
  Mbps. Home's requested dimensions select the matching RTSP source; H.264 is
  always packet-copied through independent source-bound SRTP/SRTCP transports. The unused
  D1-only Sub Stream 1 is disabled.
- **Snapshots**: Home app tile images served from the camera's `snapshot.cgi`
  via the HAP `POST /resource` endpoint. Snapshot resolution is explicitly
  configured and verified against both encoder readback and the dimensions of
  a real JPEG. On models that report `SupportIndividualResolution=false`, it
  follows the selected main/HSV recording resolution. JPEG quality is set to
  the camera maximum; malformed or black refreshes retain the last good tile.
  For transport debugging,
  `--save-snapshots` writes the most recently served JPEG to
  `<camera-name>.jpg` (disabled by default).
- **AI detection events**: the camera's `eventManager.cgi` stream
  (`SmartMotionHuman`, `SmartMotionVehicle`, `CrossLineDetection`,
  `CrossRegionDetection`) feeds two HomeKit motion sensors — one for people,
  one for vehicles — usable in Home automations and notifications.
- **HomeKit Secure Video recording**: a Rust-owned pipeline implementing the
  [secure-video specification](https://github.com/bauer-andreas/secure-video-specification) —
  camera operating mode, recording management, HomeKit Data Stream transport
  (HKDF-SHA512 + ChaCha20-Poly1305 framing, DataStream binary encoding), and
  motion-triggered fragmented-MP4 delivery with at least a 4 s prebuffer, extended
  backward to an actual IDR. Video stays packet-copied from the Home-selected main
  mode, up to 4K. Audio is decoded, continuously resampled and re-encoded to mono
  32/48-kHz AAC-LC; it is not lossless packet-copy. Clip-local mux jobs provide AAC
  preroll and covered whole-AU endings, with source/configuration fences through
  the final HDS write. Streams finish after the post-motion tail, 60 seconds, or
  a size-driven cut. Continued download requires another controller stream request.
  Amcrust configures the main video encoder and keeps acquisition AAC at 48 kHz;
  recording output-rate changes do not reconfigure that source. Requires a home hub
  and iCloud+; actual Home playback, seek and event continuation still need acceptance.
  See the [HDS wire reference](docs/hds-wire-format.md).
- **Independent live control**: RTP Stream Management Active now stops all live sessions
  served by that service without disabling HSV recording or snapshots. It is persisted,
  restored before startup exposure, and cannot override camera-off or shutdown. Re-enable
  requires fresh setup; actual Home mode/automation acceptance remains pending.
- **Camera-off ownership**: HomeKit Camera Active closes live admission, retires shared
  RTSP sources and joins live/recording workers before acknowledgment. Re-enable needs
  fresh live setup; shutdown cannot reopen admission. Snapshot epochs extend through
  cache publication and encrypted HTTP writes, including queued copies and off/on overlap.
  Revoking a partially delivered image closes that connection rather than appending to
  truncated ciphertext. Other Home modes and actual Apple behavior remain unqualified.
- **Optional audio** (`--audio`): the camera's main-stream 48 kHz AAC audio is
  transcoded to the Opus format negotiated by HomeKit. Live audio carries RTP
  and RTCP in both directions through an SRTP/SRTCP proxy, including sender
  reports and controller feedback. Recording AAC-LC processing is independent of
  live Opus and is toggled by the Home app's recording-audio switch.
- **Encoder transitions**: affected main-source media is retired before runtime settings
  writes. Failure leaves main acquisition and recording blocked until a successful retry;
  unrelated substream video stays owned independently. Live formats refresh after success,
  and unsupported dimensions/frame rates are refused rather than substituted. Recording-only
  AAC output changes do not reconfigure the camera. Firmware/Apple transition acceptance
  remains pending.
- **Consistent camera profile**: on every connection, amcrust checks and, when
  necessary, normalizes the live substream, microphone/main audio track, image
  profiles, enhancement/ROI/crop controls, exposure, day/night switching,
  denoise, IR lighting, color, orientation, sharpness, white balance, and
  burned-in overlays. Unsupported model-specific fields are skipped and every
  changed media setting is read back after application. Timestamp sizing is
  identical for the main stream, substreams, and snapshots; SmartMotion is
  enabled for people and vehicles over the full frame, and obsolete face/IVS
  rules are disabled.
- **Health and metrics**: a separate HTTP listener serves JSON at `/health` and
  Prometheus text exposition at `/metrics`, suitable for Prometheus or
  VictoriaMetrics scraping. Metrics cover uptime, camera events, errors,
  snapshots, reconnects, open connections, motion, and live/recording video.

## Building

Cargo needs the repository-pinned Nix environment for the native FFmpeg libraries
and build tools. With direnv's shell hook enabled, approve the checked-in `.envrc`
once; entering the repository then loads that environment automatically:

```sh
direnv allow
cargo check --all-targets
cargo build --release
```

For shells or editor tasks without a direnv hook, use `direnv exec . cargo check
--all-targets` (after approval), or run Cargo explicitly through Nix:

```sh
nix develop --command cargo check --all-targets
nix develop --command cargo build --release
# Or build the packaged application:
nix build .#amcrust
```

A bare Cargo command outside this environment cannot discover the Nix FFmpeg
libraries and may report missing `pkg-config` or `libavutil`. Do not install a
separate system FFmpeg to work around that. `.envrc` does not load camera credentials;
the application continues to read them from `.env` or its existing environment.

The shell/package supplies FFmpeg 9 libraries, pkg-config and libclang/bindgen;
`ffmpeg-sys-next` is pinned in Cargo. The Rust media library includes an encoder
registry, joined reader supervision, bounded packet subscriptions and source-clock
diagnostics, per-viewer H.264 RTP packetization, finite fMP4 mux jobs and AAC-to-Opus
processing. `src/main.rs` injects the same main source into live audio/video and HSV;
`src/stream/shared.rs` owns packetization, report scheduling and joined cancellation.
Video uses explicit host-monotonic read-time presentation, preserving camera timestamps
as diagnostics rather than trusting them for playback. This is not capture-time accuracy
or Apple A/V acceptance. Source-bound SRTP/SRTCP writes fence generation and
subscription retirement. Both tracks share a session monotonic domain and host-UTC
reports. Rust audio uses bounded continuous resampling, explicit startup history,
and complete Opus packets. Internal timing stays at 48 kHz; the HomeKit boundary
maps RTP and sender reports to the negotiated audio rate. The old audio subprocess, relay and
timestamp rescaler are removed. Recording consumes the same acquisition through
bounded history and joined clip-local Rust mux jobs; it no longer opens a separate reader.
The user confirmed the negotiated-rate clock correction fixed Home live-audio gaps.
A separate historical live-audio candidate anchors sample progression to AAC source timing
and supports 8/16/24/48-kHz Opus output with 20/40/60-ms frames. It refuses excessive
phase error, stale sender reports and uncovered finite endpoints; strict timing
remains the default. Priming/tail contracts are explicit, not assumed to work over
HomeKit RTP.
See the [clock decision and alternatives](docs/architecture.md#production-timing-and-recovery-decision)
for limits, acceptance work and symptoms that would require revisiting the choice.
The first sustained clock attempt failed with a source-clock refusal; a later short
success is not qualification. The existing camera check supports an explicit bounded
window (requires coordinated camera access and the existing credentials):

```sh
AMCRUST_CLOCK_SECONDS=1800 direnv exec . cargo test --test camera_audio \
  real_anchored_audio_clock -- --ignored --nocapture --test-threads=1
```

This stops on the first refusal and reports failure, without restarting acquisition
or increasing timing budgets. Source snapshots retain the first offending timing
metadata; no captured media is saved. See [Evidence](docs/evidence.md#sustained-clock-attempt-and-first-failure-provenance)
for the failed attempt and separate diagnostic outcome.

The retained strict diagnostic source mode offers audio-only `Tracks::AudioSampleClock` subscriptions
for classified, bounded AAC sender-report remapping. Original timestamps stay intact;
strict audio/recording subscriptions fail locally instead of stopping video for that
case. The processor still enforces its original ±100-ms phase and 15-second freshness
budgets. Real input has now exercised that isolation, but this is not production
clock qualification: excess cumulative phase still stops unsupported audio. Oversized
but explained AAC mapping changes now quarantine audio for the generation rather than
stop healthy video; this new quarantine path still awaits a live oversized event.
Video's own timing faults remain fatal. Production receiver feedback is now parsed in
Rust instead of returned to FFmpeg, and live media owners join producer/transport
teardown. These strict timing refusals no longer govern production video; its explicit
presentation mode still enforces packet identity, codec/parameter validity, no B-frame
reordering guesses and source retirement. Recording uses explicit recovered video
cadence and AAC timing, preserving the same source evidence. Diagnostics separate reconstructed RTP duration debt,
SR mapping changes and unexplained residuals rather than blaming all drift on SRs.

All project-owned C workers and the obsolete Python/AMC2 fixture harness are removed;
their findings and numeric records remain in [Evidence](docs/evidence.md). The read-only
production workload sampler `scripts/ingest_baseline.py` is retained, along with its
checks (`python3 -m unittest discover -s scripts -p 'test_*.py'`). The last recording
FFmpeg reader/mux command and timestamp bitstream filters are now removed as well.
No system-wide FFmpeg or compiler installation is needed with this workflow.

## Running

Requires the pinned FFmpeg libraries. The `ffmpeg` executable remains on `PATH` for
JPEG snapshot scaling, not live or recording camera acquisition.

```sh
AMCREST_USERNAME=admin AMCREST_PASSWORD=... \
amcrust --name frontyard --host 192.168.1.50
```

### Verify camera settings directly

`apply-settings` connects to one camera and exits without starting HomeKit,
event listeners, or health servers. It is read-only unless
`--write` is explicitly supplied:

```sh
amcrust --name frontyard --host 192.168.1.50 apply-settings
amcrust --name frontyard --host 192.168.1.50 apply-settings --write
```

Credentials use `AMCREST_USERNAME` and `AMCREST_PASSWORD`, as with the normal
server. The write form applies and reads back the media, overlay, native live
streams, recording encoder, snapshot/JPEG, and motion profiles. It then runs a
five-second recording probe through the same shared-source history, recovered AAC
and clip-local Rust mux path, without retaining a recording (`--probe-seconds` accepts 1..90). It continues after individual
failures, prints `PASS`/`FAIL` for every category, and exits nonzero if anything
failed. Stop the camera's normal amcrust instance first so the two processes do
not change settings concurrently.

Credentials can also live in a `.env` file. Options (all settable via env vars):

| flag               | env                | default            |                                                                 |
| ------------------ | ------------------ | ------------------ | --------------------------------------------------------------- |
| `--name`           | `CAMERA_NAME`      | —                  | accessory name                                                  |
| `--host`           | `CAMERA_HOST`      | —                  | camera IP/hostname                                              |
| `--username`       | `AMCREST_USERNAME` | —                  | camera API user                                                 |
| `--password`       | `AMCREST_PASSWORD` | —                  | camera API password                                             |
| `--port`           | `HAP_PORT`         | free port, persisted | prefers 51826–51925; unique per instance                       |
| `--hds-port`       | `HDS_PORT`         | OS-assigned        | Secure Video data-stream TCP port; must pass the firewall       |
| `--pin`            | `HAP_PIN`          | randomly generated | override the persisted setup PIN (`1234-5678`)                  |
| `--data-dir`       | `DATA_DIR`         | `./data`           | pairing state (`<data-dir>/<name>/`)                            |
| `--rtsp-subtype`   | `RTSP_SUBTYPE`     | `2`                | native allocation uses Sub Stream 2; other values are overridden |
| `--audio`          | `AUDIO`            | `true`             | send Opus audio                                                 |
| `--ir-lighting`    | `IR_LIGHTING`       | `true`             | allow automatic IR illumination; disable behind glass          |
| `--metrics-port`   | `METRICS_PORT`     | OS-assigned        | `/health` and `/metrics` HTTP port; set explicitly for scraping |
| `--save-snapshots` | `SAVE_SNAPSHOTS`   | `false`            | write the last served JPEG to `<camera-name>.jpg`               |

The selected metrics address is logged at startup. To use a stable port:

```sh
amcrust --name frontyard --host 192.168.1.50 --metrics-port 9090
curl http://localhost:9090/health
curl http://localhost:9090/metrics
```

A compact process summary is also written directly to stderr every hour,
regardless of the configured log filter.

For measurement boundaries and historical findings, see [Evidence](docs/evidence.md).
The [TODO](docs/todo.md#4-run-and-evaluate-the-real-replacement-alongside-integration)
contains the real-device cutover workflow. Startup metrics measure sent RTP/IDR,
not controller decode latency; an end-of-stream write is not confirmed iCloud storage.

On first startup, each camera gets its own random setup PIN, persisted with its
pairing state. The log prints it as `1234-5678`; add the accessory in the Home
app via "Add Accessory → More options…". The instance must be reachable from
your iOS devices/Home hub on the same network (mDNS + UDP).

Pairing state lives under `DATA_DIR` — keep it across restarts and deploys, or
the accessory will have to be removed and re-paired in the Home app.

## NixOS

The flake exposes the package through `overlays.default` and the camera service
through `nixosModules.default`. Import both into a NixOS configuration:

```nix
{
  inputs.amcrust.url = "github:bitnixdev/amcrust";

  outputs = {nixpkgs, amcrust, ...}: {
    nixosConfigurations.camera-host = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        amcrust.nixosModules.default
        {
          nixpkgs.overlays = [amcrust.overlays.default];

          services.amcrust = {
            enable = true;
            openFirewall = true;
            cameras.frontyard = {
              host = "192.168.1.50";
              passwordFile = "/run/secrets/amcrest-frontyard";
              irLighting = false; # camera is behind glass
              hapPort = 51826;
              hdsPort = 51926;
              metricsPort = 9090;
            };
          };
        }
      ];
    };
  };
}
```

`passwordFile` must contain only the camera API password. It is loaded through
systemd's credential mechanism and is not copied into the Nix store. Each
camera creates an independent `amcrust-<name>.service`; pairing state is kept
under `/var/lib/amcrust` by default.

Secure Video uses a separate HDS TCP listener. With a fixed `hapPort`, its
default is `hapPort + 100`; both ports must pass the firewall. Set
`services.amcrust.openFirewall = true`, or allow the HAP and HDS ports in
interface-specific firewall rules.

## Current code map

The target architecture and invariants live only in [Architecture](docs/architecture.md).
This map describes the existing application, not the unfinished cutover draft.

```
src/
  main.rs       one camera instance: config, wiring, HAP server startup
  amcrest.rs    digest-auth camera client: snapshots, RTSP URLs, event stream
  accessory.rs  HomeKit accessory: stream management + motion sensor services
  stream.rs     SetupEndpoints/SelectedRTPStreamConfiguration TLV8 negotiation
                with shared Rust video/audio and joined codec workers
  srtp.rs       SRTP/SRTCP primitives
  media/transport.rs  shared secure UDP writes, accepted counts and session-stop fence
  media/rtcp.rs  compound sender reports, session CNAME and receiver feedback
  hsv/          recording configuration, fMP4 prebuffer, HDS codec/framing/delivery
  metrics.rs    health, media metrics and delivery outcomes
  motion.rs     AI events → motion sensor characteristic updates
  tlv8.rs       minimal TLV8 encoder/decoder
hap/            vendored fork of hap-rs (github.com/ewilken/hap-rs), extended
                with: POST /resource snapshot endpoint, base64 encoding for
                tlv8/data characteristic values, maintained if-addrs dep
```

The vendored `hap` fork is necessary because upstream hap-rs has no camera
support: it lacks the snapshot resource endpoint and transported TLV8
characteristic values as JSON byte arrays instead of base64 strings.
