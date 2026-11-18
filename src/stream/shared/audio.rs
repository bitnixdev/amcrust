//! Per-viewer HomeKit audio transport over shared, generation-bound Opus output.
mod encoders;
pub(crate) use encoders::Encoders;
#[cfg(test)]
mod tests;
use super::*;
use amcrust::{
    ingest::Cancellation,
    media::audio::{Config, Pool, Transcoder},
};

/// HomeKit's negotiated-rate wire clock is distinct from Opus's canonical
/// 48-kHz sample/presentation domain. Convert signed, unwrapped presentation
/// offsets before adding the independent RTP origin; never divide wrapped RTP.
/// See go2rtc/pkg/opus/homekit.go for the HAP RFC 3550 timestamp exception.
struct HapClock {
    presentation: PresentationClock,
    origin: u32,
    rate: u32,
}
impl HapClock {
    fn new(presentation: PresentationClock, origin: u32, rate: u32) -> Result<Self, Failure> {
        if !matches!(rate, 8000 | 16000 | 24000 | 48000) {
            return Err("unsupported HomeKit audio clock".into());
        }
        Ok(Self {
            presentation,
            origin,
            rate,
        })
    }
    fn timestamp(&self, presentation_48k: i64) -> u32 {
        let ticks = (i128::from(presentation_48k) * i128::from(self.rate)).div_euclid(48000);
        self.origin.wrapping_add(ticks as u32)
    }
    fn report(&self) -> Result<rtcp::ClockPair, amcrust::media::Error> {
        self.presentation.report(self.origin, self.rate)
    }
}

pub(crate) struct Audio {
    pub clock: PresentationClock,
    pub source: Source,
    pub socket: tokio::net::UdpSocket,
    pub peer: SocketAddr,
    pub key: Vec<u8>,
    pub ssrc: u32,
    pub identity: [u8; 16],
    pub mtu: usize,
    pub payload: u8,
    pub interval: Duration,
    pub config: Config,
    pub pool: Pool,
    pub encoders: Encoders,
}
pub(crate) fn spawn(audio: Audio, metrics: Arc<Metrics>) -> MediaProxy {
    let source = audio.source.clone();
    let control = Control::pending();
    let owner = control.clone();
    let task = tokio::spawn(async move {
        let _finish = Finish(owner.clone());
        if !matches!(audio.config.frame_ms, 20 | 40 | 60) {
            metrics.error(ErrorSubsystem::LiveStream);
            warn!("unsupported Opus packet time: {}", audio.config.frame_ms);
            return;
        }
        let mut feed = audio.encoders.attach(&audio);
        let result = tokio::select! {
            biased;
            _ = owner.cancelled() => Ok(()),
            r = run(audio, &mut feed, &owner, &metrics) => r,
        };
        owner.revoke();
        feed.leave().await;
        if let Err(error) = result {
            metrics.error(ErrorSubsystem::LiveStream);
            warn!("shared audio stopped: {error}");
        }
    });
    MediaProxy {
        source,
        transport: control,
        task: Some(task),
    }
}
async fn run(
    audio: Audio,
    feed: &mut encoders::Feed,
    control: &Control,
    metrics: &Metrics,
) -> Result<(), Failure> {
    if !(96..=127).contains(&audio.payload) {
        return Err("unsupported audio payload type".into());
    }
    let clock = feed.clock();
    // Cold startup may spend up to ten seconds discovering camera tracks.
    let first = tokio::time::timeout(Duration::from_secs(15), feed.recv()).await??;
    let transport = Arc::new(Transport::new_opus_bound(
        audio.socket,
        audio.peer,
        &audio.key,
        audio.ssrc,
        audio.mtu,
        &first,
    )?);
    control.install(transport.clone())?;
    let reports = rtcp::Sender::new(audio.ssrc, audio.identity);
    let mut receiver = rtcp::Receiver::new(audio.ssrc);
    let mut interval = tokio::time::interval(audio.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let wire_clock = HapClock::new(clock, rand::random(), audio.config.rate)?;
    debug!(
        "HomeKit Opus: encoder={} Hz, RTP/RTCP={} Hz, packet={} ms; internal presentation=48000 Hz",
        audio.config.rate, audio.config.rate, audio.config.frame_ms
    );
    let mut sequence: u16 = rand::random();
    let mut bytes = vec![0; transport.rtp_capacity()];
    let mut sent = false;
    let mut packet = first;
    loop {
        {
            let timestamp = wire_clock.timestamp(packet.live_timestamp_48k()?);
            let payload = packet.data()?;
            if payload.len() + 12 > bytes.len() {
                return Err(TransportError::TooLarge.into());
            }
            bytes[0] = 0x80;
            bytes[1] = audio.payload | if sent { 0 } else { 0x80 };
            bytes[2..4].copy_from_slice(&sequence.to_be_bytes());
            bytes[4..8].copy_from_slice(&timestamp.to_be_bytes());
            bytes[8..12].copy_from_slice(&audio.ssrc.to_be_bytes());
            bytes[12..12 + payload.len()].copy_from_slice(payload);
            transport
                .send_rtp(&bytes[..12 + payload.len()], || packet.data().is_ok())
                .await?;
            sequence = sequence.wrapping_add(1);
            sent = true;
        }
        drop(packet);
        loop {
            tokio::select! {
                next = feed.recv() => { packet = next?; break; },
                _ = interval.tick(), if sent => {
                    transport.send_rtcp(&reports.report(wire_clock.report()?, transport.rtp_counts()), || true).await?;
                },
                feedback = transport.feedback() => {
                    let packet = match feedback {
                        Ok(packet) => packet,
                        Err(TransportError::Authentication | TransportError::TooLarge) => { metrics.feedback_refused(); continue; }
                        Err(error) => return Err(error.into()),
                    };
                    match receiver.receive(&packet) {
                        Ok(feedback) => {
                            metrics.receiver_feedback(feedback);
                            if feedback.bye {
                                return Ok(());
                            }
                        },
                        Err(_) => metrics.feedback_refused(),
                    }
                }
            }
        }
    }
}
