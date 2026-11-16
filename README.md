# amcrust

Bridges a single Amcrest camera to HomeKit as a native camera/video accessory,
written in Rust. Run **one instance per camera** — each instance is its own
standalone HomeKit accessory with independent pairing state, so one camera
failing or being reconfigured never affects the others.

Supported/tested cameras: Amcrest `IP8M-2696E-AI` and `IP8M-2796E-AI`.

## Project direction and documentation

**The shared-ingest cutover is not implemented yet.** The current application below
still uses separate FFmpeg camera readers. The destination is Rust-owned shared
acquisition through FFmpeg library bindings, with independent HomeKit transports—
not project-owned C workers, MediaMTX, or a selectable legacy-reader fallback.

- [Architecture](docs/architecture.md): current versus intended data flow and decisions.
- [TODO](docs/todo.md): the single ordered backlog, including every C replacement/removal
  and the authorized real-device testing workflow. No more synthetic-testing work.
- [Evidence](docs/evidence.md): consolidated completed findings and unresolved limits;
  historical experiment success is not production or Apple-client acceptance.
- [HDS wire reference](docs/hds-wire-format.md): transport/protocol reference, not a roadmap.

## What the current application does

- **Live video in the Home app**: HAP RTP stream management with SRTP. Amcrust
  advertises and routes two native camera encoders without video transcoding:
  the HSV-controlled main stream for 1080p and Sub Stream 2 at 720p/15 fps/2
  Mbps. Home's requested dimensions select the matching RTSP source; H.264 is
  always packet-copied through an RTP/RTCP-multiplexing SRTP proxy. The unused
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
- **HomeKit Secure Video recording**: full implementation of the
  [secure-video specification](https://github.com/bauer-andreas/secure-video-specification) —
  camera operating mode, recording management, HomeKit Data Stream transport
  (HKDF-SHA512 + ChaCha20-Poly1305 framing, DataStream binary encoding), and
  motion-triggered fragmented-MP4 delivery with a 4 s prebuffer. Recordings are
  taken from the camera's best Home-selected main-stream mode, up to 4K, with
  **no transcoding**. Amcrust advertises only modes reported by the camera,
  reprograms the main-stream encoder (resolution, fps, GOP = fragment length,
  bitrate, AAC sample rate) to match, and ffmpeg stream-copies into fMP4. Requires a home
  hub and iCloud+; see the [HDS wire reference](docs/hds-wire-format.md).
- **Optional audio** (`--audio`): the camera's main-stream 48 kHz AAC audio is
  transcoded to the Opus format negotiated by HomeKit. Live audio carries RTP
  and RTCP in both directions through an SRTP/SRTCP proxy, including sender
  reports and controller feedback. Recording audio is AAC-LC stream-copy
  (toggled by the Home app's recording-audio switch).
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
processing. These are not yet connected to the production HomeKit paths. An explicit
live-audio candidate anchors decoded-sample progression to 48-kHz AAC source timing
and supports 8/16/24/48-kHz Opus output with 20/40/60-ms frames. It refuses excessive
phase error, stale sender reports and uncovered finite endpoints; strict timing
remains the default. Priming/tail contracts are explicit, not assumed to work over
HomeKit RTP.
See the [clock decision and alternatives](docs/architecture.md#candidate-live-audio-decision-and-revisit-guide)
for limits, acceptance work and symptoms that would require revisiting the choice.
The unused project C worker draft is removed.
No system-wide FFmpeg or compiler installation is needed with this workflow.

## Running

Requires `ffmpeg` on `PATH` (with SRTP support; standard builds have it).

```sh
AMCREST_USERNAME=admin AMCREST_PASSWORD=... \
amcrust --name frontyard --host 192.168.1.50
```

### Verify camera settings directly

`apply-settings` connects to one camera and exits without starting HomeKit,
recording, event listeners, or health servers. It is read-only unless
`--write` is explicitly supplied:

```sh
amcrust --name frontyard --host 192.168.1.50 apply-settings
amcrust --name frontyard --host 192.168.1.50 apply-settings --write
```

Credentials use `AMCREST_USERNAME` and `AMCREST_PASSWORD`, as with the normal
server. The write form applies and reads back the media, overlay, native live
streams, recording encoder, snapshot/JPEG, and motion profiles. It then runs a
five-second real-camera H.264/AAC packet-copy probe through the exact production
fMP4 command without retaining a recording. It continues after individual
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
                and the ffmpeg RTSP→SRTP media pipeline
  srtp.rs       per-session SRTP/SRTCP handling
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
