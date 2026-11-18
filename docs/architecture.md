# Media architecture

One amcrust instance serves one camera. A Rust registry owns one interruptible
libavformat acquisition per active hardware encoder. Consumers subscribe to
encoded packets, not RTSP URLs. Recording, live audio and main-stream live video
share the main acquisition. Sub Stream 2 is a separate native live video source.

- **Acquisition:** `src/ingest.rs` owns FFmpeg packet/configuration lifetimes.
  `src/source/` handles admission, bounded queues, byte/slot budgets and generation
  retirement. Dropping demand lets the reader idle out. A fresh acquisition never
  revives permissions issued by a retired generation.
- **Timing:** production uses host-monotonic presentation time. Keep the original
  camera PTS/DTS and sender reports as diagnostic evidence, not as trusted wall
  time. Reordered video is refused rather than guessing a presentation order.
  Audio uses bounded continuous resampling; internal Opus timing is in 48-kHz
  units. Both RTP timestamps and sender reports use the negotiated HomeKit wire rate.
- **Live video:** match native dimensions and a frame rate within the selected
  maximum. Each viewer owns its H.264 packetizer, transport keys, RTP sequence and
  clock origin, sender reports and feedback state. No video transcoding or relay.
- **Live audio:** a manager shares a joined AAC-to-Opus worker across matching
  codec settings. Each worker holds one generation-bound subscription. A joining
  viewer takes up to 80 ms (rounded up to whole packets) of decoder history and
  then receives live packets without a gap. Lag closes only that receiver;
  the last viewer stops and joins the worker. Each viewer owns its secure transport.
- **Recording:** bounded history retains encoded video and recovered AAC. Each
  clip starts at an actual IDR, uses a common timeline and gets its own finite
  libavformat mux job and HDS writes. History, codec and mux jobs have bounded
  reservations; kernel-accepted output is not a storage acknowledgement.
- **Configuration and privacy:** retire affected source generations before changing
  camera encoders. Camera Active gates acquisition, recording and snapshots; RTP
  Service Active gates live streams separately. Final transport writes are fenced
  against retirement, including copied output. Shutdown signals and joins owners.
- **Snapshots:** fetch JPEGs via the camera HTTP endpoint. Decode, fit without
  upscaling, reject nearly black frames and encode previews in-process with the
  Cargo JPEG library. Snapshot epochs guard cache publication and HTTP delivery.

FFmpeg is a linked library dependency, not a production executable. See the
[HDS wire reference](hds-wire-format.md) for framing and [Evidence](evidence.md)
for measurements and historical design experiments.
