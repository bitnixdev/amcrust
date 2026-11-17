//! Generation-bound recording media. Video remains encoded; AAC is recovered
//! continuously before clip selection. Original acquisition metadata is immutable.
#[cfg(test)]
#[path = "recording_tests.rs"]
mod tests;
use super::{
    Error,
    audio::{AacPacket, AacTranscoder, Pool},
    audio_recovery::Recovery,
    presentation::PresentationClock,
};
use crate::{
    ingest::{Cancellation, Codec},
    source::{Delivery, DeliveryGuard, Timing, nals},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub width: u16,
    pub height: u16,
    pub fps: u8,
    pub audio_rate: Option<u32>,
    pub audio_bitrate: u32,
}
/// One immutable normalized access unit. Keeping it keeps the original video
/// delivery charges or the bounded AAC output slot. It never opens a reader.
pub enum Frame {
    Video {
        delivery: Arc<Delivery>,
        pts: i64,
        duration: i64,
        idr: bool,
    },
    Audio(AacPacket),
}
impl Frame {
    pub fn position(&self) -> Result<(i64, i64, u32), Error> {
        match self {
            Self::Video {
                delivery,
                pts,
                duration,
                ..
            } => {
                delivery.packet()?;
                Ok((*pts, *duration, 90000))
            }
            Self::Audio(packet) => packet.position(),
        }
    }
    pub fn is_idr(&self) -> bool {
        matches!(self, Self::Video { idr: true, .. })
    }
    pub fn bytes(&self) -> Result<usize, Error> {
        match self {
            Self::Video { delivery, .. } => Ok(delivery.packet()?.owned_bytes()),
            Self::Audio(packet) => Ok(packet.data()?.len() + 256),
        }
    }
    pub fn start_ns(&self) -> Result<i128, Error> {
        let (pts, _, rate) = self.position()?;
        Ok(i128::from(pts) * 1_000_000_000 / i128::from(rate))
    }
    pub fn end_ns(&self) -> Result<i128, Error> {
        let (pts, duration, rate) = self.position()?;
        Ok((i128::from(pts) + i128::from(duration)) * 1_000_000_000 / i128::from(rate))
    }
}

struct VideoClock {
    first: Instant,
    last: Instant,
    fps: u8,
    clock: PresentationClock,
    recovery: Option<Recovery>,
    origin: i128,
    position: i128,
    warm_frames: u64,
    warm_origin: Option<Instant>,
}
impl VideoClock {
    fn new(first: Instant, fps: u8, clock: PresentationClock) -> Self {
        Self {
            first,
            last: first,
            fps,
            clock,
            recovery: None,
            origin: 0,
            position: 0,
            warm_frames: 0,
            warm_origin: None,
        }
    }
    fn observe(&mut self, at: Instant, idr: bool) -> Result<Option<(i64, i64)>, Error> {
        if at < self.last {
            return Err(Error::Clock);
        }
        self.last = at;
        if self.recovery.is_none() {
            if at.saturating_duration_since(self.first) > Duration::from_secs(15) {
                return Err(Error::Deadline);
            }
            // Counted pre-admission calibration only: lower arrival envelope
            // prevents an IDR serialization burst from becoming a permanent anchor.
            let elapsed = Duration::from_nanos(
                self.warm_frames
                    .checked_mul(1_000_000_000)
                    .ok_or(Error::Limit)?
                    / u64::from(self.fps),
            );
            self.warm_frames += 1;
            let candidate = at.checked_sub(elapsed).ok_or(Error::Clock)?;
            let origin = self.warm_origin.map_or(candidate, |old| old.min(candidate));
            self.warm_origin = Some(origin);
            if !idr
                || at.saturating_duration_since(self.first) < Duration::from_secs(1)
                || self.warm_frames <= u64::from(self.fps)
            {
                return Ok(None);
            }
            let anchor = origin + elapsed;
            self.recovery = Some(Recovery::new(anchor));
            self.origin = self.clock.ticks(anchor, 90000);
        }
        let denominator = i128::from(self.fps) * 1_000_000;
        let pts = self.position / denominator;
        let recovery = self.recovery.as_mut().unwrap();
        recovery.step(at, self.position * 48000 / (denominator * 90000), 90000)?;
        self.position += 90000 * (1_000_000 + i128::from(recovery.status().correction_ppm));
        let next = self.position / denominator;
        Ok(Some((
            i64::try_from(self.origin + pts).map_err(|_| Error::Clock)?,
            i64::try_from(next - pts).map_err(|_| Error::Clock)?,
        )))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Status {
    pub startup_video_frames: u64,
    pub startup_audio_samples: Option<u64>,
    pub video: Option<super::audio::RecoveryStatus>,
    pub audio: Option<super::audio::RecoveryStatus>,
}
pub struct Processor {
    source: DeliveryGuard,
    config: Config,
    clock: PresentationClock,
    video: VideoClock,
    failure: Option<Error>,
    audio: Option<AacTranscoder>,
    pool: Pool,
    cancellation: Cancellation,
}
impl Processor {
    pub fn new(
        first: &Delivery,
        config: Config,
        pool: Pool,
        cancellation: Cancellation,
    ) -> Result<Self, Error> {
        if first.timing()? != Timing::Presentation
            || !(1..=60).contains(&config.fps)
            || config
                .audio_rate
                .is_some_and(|rate| !matches!(rate, 32000 | 48000))
        {
            return Err(Error::Configuration);
        }
        let stream = &first.configuration()?[first.packet()?.stream_index()];
        if stream.codec != Codec::H264
            || stream.parameters.dimensions() != (i32::from(config.width), i32::from(config.height))
        {
            return Err(Error::Configuration);
        }
        let clock = PresentationClock::new();
        Ok(Self {
            source: first.guard()?,
            config,
            clock,
            video: VideoClock::new(first.packet()?.received_at(), config.fps, clock),
            failure: None,
            audio: None,
            pool,
            cancellation,
        })
    }
    pub fn status(&self) -> Status {
        Status {
            startup_video_frames: self
                .video
                .warm_frames
                .saturating_sub(u64::from(self.video.recovery.is_some())),
            startup_audio_samples: self
                .audio
                .as_ref()
                .map(|audio| audio.stats().preroll_samples),
            video: self.video.recovery.as_ref().map(Recovery::status),
            audio: self.audio.as_ref().and_then(AacTranscoder::recovery_status),
        }
    }
    pub fn push(&mut self, delivery: Delivery) -> Result<Vec<Arc<Frame>>, Error> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let result = self.push_inner(delivery);
        if let Err(error) = result {
            self.failure = Some(error);
        }
        result
    }
    fn push_inner(&mut self, delivery: Delivery) -> Result<Vec<Arc<Frame>>, Error> {
        self.source.accepts(&delivery)?;
        if self.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let packet = delivery.packet()?;
        let codec = delivery.configuration()?[packet.stream_index()].codec;
        match codec {
            Codec::Aac => {
                let Some(rate) = self.config.audio_rate else {
                    return Ok(Vec::new());
                };
                if self.audio.is_none() {
                    self.audio = Some(AacTranscoder::new(
                        &delivery,
                        rate,
                        self.config.audio_bitrate,
                        self.clock,
                        &self.pool,
                        self.cancellation.clone(),
                    )?);
                }
                Ok(self
                    .audio
                    .as_mut()
                    .unwrap()
                    .push(&delivery)?
                    .into_iter()
                    .map(|packet| Arc::new(Frame::Audio(packet)))
                    .collect())
            }
            Codec::H264 => {
                let at = packet.received_at();
                let idr = nals(packet.data())?.iter().any(|nal| nal[0] & 31 == 5);
                let Some((pts, duration)) = self.video.observe(at, idr)? else {
                    return Ok(Vec::new());
                };
                Ok(vec![Arc::new(Frame::Video {
                    delivery: Arc::new(delivery),
                    pts,
                    duration,
                    idr,
                })])
            }
        }
    }
}
