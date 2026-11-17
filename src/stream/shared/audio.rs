//! Joined thread-bound AAC/Opus worker. Shares acquisition, not transport/codec state.
#[cfg(test)]
mod tests;
use super::*;
use amcrust::{
    ingest::Cancellation,
    media::audio::{Config, Pool, Transcoder},
};

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
}
pub(crate) fn spawn(audio: Audio, metrics: Arc<Metrics>) -> MediaProxy {
    let control = Control::pending();
    let owner = control.clone();
    let runtime = tokio::runtime::Handle::current();
    let task = tokio::spawn(async move {
        let _finish = Finish(owner.clone());
        // Never abort/detach the blocking codec worker. Control cancellation wakes
        // its I/O; stop() awaits this join before acknowledging native teardown.
        let worker_control = owner.clone();
        let worker_metrics = metrics.clone();
        let result = tokio::task::spawn_blocking(move || {
            runtime.block_on(async {
                tokio::select! {
                    biased;
                    _ = worker_control.cancelled() => Ok(()),
                    result = run(audio, &worker_control, &worker_metrics) => result,
                }
            })
        })
        .await;
        if !matches!(result, Ok(Ok(()))) && !owner.is_closed() {
            metrics.error(ErrorSubsystem::LiveStream);
            warn!("shared audio stopped: {result:?}");
        }
        owner.revoke();
    });
    MediaProxy {
        transport: control,
        task: Some(task),
    }
}
async fn run(audio: Audio, control: &Control, metrics: &Metrics) -> Result<(), Failure> {
    if !(96..=127).contains(&audio.payload) {
        return Err("unsupported audio payload type".into());
    }
    let clock = audio.clock;
    let mut subscription = audio
        .source
        .subscribe(Tracks::Audio, SubscriptionLimits::default())
        .await?;
    let mut delivery = subscription.recv().await?;
    let transport = Arc::new(Transport::new_source_bound(
        audio.socket,
        audio.peer,
        &audio.key,
        audio.ssrc,
        audio.mtu,
        &delivery,
    )?);
    control.install(transport.clone())?;
    let mut codec = Transcoder::new_presented(
        &delivery,
        audio.config,
        clock,
        &audio.pool,
        Cancellation::default(),
    )?;
    let reports = rtcp::Sender::new(audio.ssrc, audio.identity);
    let mut receiver = rtcp::Receiver::new(audio.ssrc);
    let mut interval = tokio::time::interval(audio.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let origin: u32 = rand::random();
    let mut sequence: u16 = rand::random();
    let mut bytes = vec![0; transport.rtp_capacity()];
    let mut sent = false;
    let startup_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if !sent && tokio::time::Instant::now() >= startup_deadline {
            return Err("audio startup deadline".into());
        }
        let output = codec.push(&delivery).map_err(|error| {
            warn!(
                "audio recovery refused input: {error}; {:?}",
                codec.recovery_status()
            );
            error
        })?;
        for packet in output {
            let timestamp = origin.wrapping_add(packet.live_timestamp_48k()? as u32);
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
        drop(delivery);
        loop {
            tokio::select! {
                next = subscription.recv() => { delivery = next?; break; },
                _ = tokio::time::sleep_until(startup_deadline), if !sent => return Err("audio startup deadline".into()),
                _ = interval.tick(), if sent => {
                    transport.send_rtcp(&reports.report(clock.report(origin, 48000)?, transport.rtp_counts()), || true).await?;
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
                                debug!("shared audio end: {:?}, {:?}", codec.stats(), codec.recovery_status());
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
