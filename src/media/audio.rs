//! Thread-bound AAC-LC -> Opus through public FFmpeg codec/resampler APIs.
//! Production owners must drive this from a blocking media worker, not an async
//! executor task. No I/O, camera reader or detached worker is opened by this layer.
//!
//! Codec sample ordinals are PRIVATE encoder bookkeeping, not source clocks.
//! Strict/source-anchored policies never asynchronously compensate samples; source
//! contradictions remain evidence. Explicit presentation mode warms decoder history
//! before one host-monotonic anchor, then continuously resamples within bounded
//! limits without silence, post-admission drops or reanchoring. Neither mode implies
//! HomeKit playback acceptance; presentation mode cannot complete a finite cut.
//!
//! This is the processing boundary, not a production audio pool/transport. Both
//! Config and ClockPolicy belong in any future generation-scoped sharing key.
pub use super::audio_clock::{ClockPolicy, ClockStatus};
pub use super::audio_recovery::RecoveryStatus;
use super::{Error, audio_clock::SampleClock, rescale};
use super::{
    audio_recovery::{Recovery, Startup},
    presentation::PresentationClock,
};
use crate::{
    ingest::{Cancellation, Codec, SenderReport, TimeBase},
    source::{Delivery, DeliveryGuard},
};
use ffmpeg_sys_next as av;
use std::{
    collections::VecDeque,
    marker::PhantomData,
    ptr,
    rc::Rc,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const FIFO_SAMPLES: i32 = 32768;
const CONVERT_SAMPLES: i32 = 16384;
const MAX_PACKET_BYTES: usize = 8192;
const RESERVATION: usize = 2 * 1024 * 1024;
/// Logical AAC job reservation: 1024 bounded 8-KiB outputs plus codec/metadata headroom.
pub const AAC_RESERVATION_BYTES: usize = 12 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Config {
    pub rate: u32,
    pub channels: u8,
    pub bitrate: u32,
    pub frame_ms: u8,
    pub vbr: bool,
    pub fec: bool,
    pub expected_loss_percent: u8,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            rate: 16000,
            channels: 1,
            bitrate: 24000,
            frame_ms: 20,
            vbr: true,
            fec: true,
            expected_loss_percent: 5,
        }
    }
}
impl Config {
    fn valid(self) -> bool {
        matches!(self.rate, 8000 | 16000 | 24000 | 48000)
            && matches!(self.channels, 1 | 2)
            && (16000..=64000).contains(&self.bitrate)
            && matches!(self.frame_ms, 20 | 40 | 60)
            && self.expected_loss_percent <= 100
    }
}
#[derive(Clone)]
pub struct Pool {
    jobs: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    capacity: usize,
    job_limit: usize,
}
impl Pool {
    pub fn new(jobs: usize, bytes: usize) -> Result<Self, Error> {
        if jobs == 0 || jobs > 64 || bytes < RESERVATION || bytes > u32::MAX as usize {
            return Err(Error::Configuration);
        }
        Ok(Self {
            jobs: Arc::new(Semaphore::new(jobs)),
            bytes: Arc::new(Semaphore::new(bytes)),
            capacity: bytes,
            job_limit: jobs,
        })
    }
    pub fn snapshot(&self) -> super::PoolSnapshot {
        super::PoolSnapshot {
            reserved_jobs: self.job_limit - self.jobs.available_permits(),
            job_limit: self.job_limit,
            reserved_bytes: self.reserved_bytes(),
            byte_limit: self.capacity,
        }
    }
    pub fn reserved_bytes(&self) -> usize {
        self.capacity - self.bytes.available_permits()
    }
}

#[cfg(test)]
mod pool_accounting_tests {
    use super::*;
    #[test]
    fn snapshots_follow_retained_reservations() {
        let pool = Pool::new(2, 4 * 1024 * 1024).unwrap();
        let observer = pool.clone();
        let job = pool.jobs.clone().try_acquire_owned().unwrap();
        let bytes = pool.bytes.clone().try_acquire_many_owned(1024).unwrap();
        drop(pool);
        let snapshot = observer.snapshot();
        assert_eq!(snapshot.reserved_jobs, 1);
        assert_eq!(snapshot.job_limit, 2);
        assert_eq!(snapshot.reserved_bytes, 1024);
        assert_eq!(snapshot.byte_limit, 4 * 1024 * 1024);
        drop((job, bytes));
        assert_eq!(observer.snapshot().reserved_jobs, 0);
        assert_eq!(observer.snapshot().reserved_bytes, 0);
    }
}

/// Continuous recovered AAC for recording. This shares the decode/resample core,
/// not Opus wire semantics. It opens no reader and cannot claim a finite tail.
pub struct AacTranscoder {
    inner: Transcoder,
    parameters: Arc<crate::ingest::CodecParameters>,
}
pub struct AacPacket {
    packet: OpusPacket,
    parameters: Arc<crate::ingest::CodecParameters>,
}
impl AacTranscoder {
    pub fn new(
        first: &Delivery,
        rate: u32,
        bitrate: u32,
        clock: PresentationClock,
        pool: &Pool,
        cancellation: Cancellation,
    ) -> Result<Self, Error> {
        let config = Config {
            rate,
            bitrate,
            ..Default::default()
        };
        let inner = Transcoder::new_timed(
            first,
            config,
            ClockPolicy::Presentation,
            Some(clock),
            pool,
            cancellation,
            true,
        )?;
        let parameters = Arc::new(
            unsafe { crate::ingest::CodecParameters::from_encoder(inner.encoder.0) }
                .ok_or(Error::Allocation)?,
        );
        Ok(Self { inner, parameters })
    }
    pub fn push(&mut self, delivery: &Delivery) -> Result<Vec<AacPacket>, Error> {
        Ok(self
            .inner
            .push(delivery)?
            .into_iter()
            .map(|packet| AacPacket {
                packet,
                parameters: self.parameters.clone(),
            })
            .collect())
    }
    pub fn stats(&self) -> Stats {
        self.inner.stats()
    }
    pub fn recovery_status(&self) -> Option<RecoveryStatus> {
        self.inner.recovery_status()
    }
}
impl AacPacket {
    pub fn data(&self) -> Result<&[u8], Error> {
        self.packet.data()
    }
    pub fn parameters(&self) -> &Arc<crate::ingest::CodecParameters> {
        &self.parameters
    }
    /// Signed common presentation coordinate at the AAC output sample rate.
    pub fn position(&self) -> Result<(i64, i64, u32), Error> {
        self.packet.check()?;
        let clock = self
            .packet
            .job
            .clock
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let rate = self.packet.job.config.rate;
        let origin = (i128::from(clock.presentation_origin.ok_or(Error::Clock)?)
            * i128::from(rate))
        .div_euclid(48000);
        let packet = unsafe { &*self.packet.raw };
        if packet.pts == av::AV_NOPTS_VALUE || packet.duration != 1024 {
            return Err(Error::Incomplete);
        }
        let pts = i64::try_from(origin + i128::from(packet.pts)).map_err(|_| Error::Clock)?;
        Ok((pts, packet.duration, rate))
    }
    pub(crate) fn as_ptr(&self) -> *const av::AVPacket {
        self.packet.raw
    }
    pub(crate) fn guard(&self) -> &DeliveryGuard {
        &self.packet.job.source
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockMismatch {
    /// Decoder-input sample units. Presentation mode uses a private ordinal;
    /// source timestamp health must instead be read from the source diagnostics.
    pub expected: i64,
    pub actual: i64,
}
#[derive(Debug, Clone, Copy, Default)]
pub struct Stats {
    pub input_packets: u64,
    /// Source-rate samples per channel, not a presentation-time counter.
    pub decoded_samples: u64,
    /// Explicit live pre-admission decoder warmup; never claimed as audible output.
    pub preroll_samples: u64,
    /// Target-rate samples per channel, including the resampler's finite tail.
    pub resampled_samples: u64,
    pub encoded_packets: u64,
    pub payload_bytes: u64,
    pub clock_discontinuities: u64,
    pub first_mismatch: Option<ClockMismatch>,
    pub minimum_step_error: Option<i128>,
    pub maximum_step_error: Option<i128>,
    pub sender_reports: u64,
    pub last_sender_report: Option<SenderReport>,
}
struct Clock {
    presentation_origin: Option<i64>,
    first_pts: Option<i64>,
    previous_end: Option<i64>,
    mismatch: Option<ClockMismatch>,
    input_rate: u32,
    converted_samples: u64,
    locally_finished: bool,
    samples: SampleClock,
}
struct Job {
    source: DeliveryGuard,
    cancellation: Cancellation,
    failure: Mutex<Option<Error>>,
    clock: Mutex<Clock>,
    header: Vec<u8>,
    config: Config,
    initial_padding: i32,
    output_residence: Duration,
    outputs: Arc<Semaphore>,
    _permits: Vec<OwnedSemaphorePermit>,
}
impl Job {
    fn fail(&self, error: Error) -> Error {
        *self
            .failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_or_insert(error)
    }
    fn check(&self) -> Result<(), Error> {
        if let Some(error) = *self.failure.lock().unwrap_or_else(|p| p.into_inner()) {
            return Err(error);
        }
        let result = if self.cancellation.is_cancelled() {
            Err(Error::Cancelled)
        } else {
            self.source.check().map_err(Error::from)
        };
        result
            .and_then(|()| {
                self.clock
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .samples
                    .check(Instant::now())
            })
            .map_err(|e| self.fail(e))
    }
}

/// Skip/discard counts in the Opus decoder's fixed 48-kHz sample domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DecoderSkip {
    pub start: u32,
    pub end: u32,
    pub start_reason: u8,
    pub end_reason: u8,
}

/// Candidate-policy presentation contract. The complete encoded packet must be
/// decoded, with these trims honored. RTP alone cannot communicate this metadata;
/// a transport lacking an explicit preroll/trim contract must refuse it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Presentation {
    pub packet_timestamp_48k: i64,
    pub audible_timestamp_48k: i64,
    pub audible_samples: u32,
    /// Effective trim, including OpusHead pre-skip when packet side data omits it.
    /// Do NOT apply it again to samples already trimmed by the decoder.
    pub decoder_skip: DecoderSkip,
    /// Additional end trim for a finite resampler endpoint rounded up by less
    /// than one encoder-rate sample. Included in decoder_skip.end, in 48-kHz units.
    pub resampler_tail_discard_48k: u32,
}

fn finite_coverage(clock: &Clock, config: Config) -> Result<(), Error> {
    if !config.valid() || clock.input_rate != 48000 {
        return Err(Error::Configuration);
    }
    let factor = (48000 / config.rate) as i128;
    let converted = clock.converted_samples as i128 * factor;
    let decoded = clock.samples.status.decoded_samples as i128;
    if converted < decoded || converted - decoded >= factor {
        return Err(Error::Incomplete);
    }
    Ok(())
}

// Arithmetic only: caller checks source ownership, policy and report freshness.
fn presentation(
    clock: &Clock,
    config: Config,
    pts: i64,
    duration: i64,
    pre_skip: u32,
    mut skip: DecoderSkip,
) -> Result<Presentation, Error> {
    if !config.valid() || clock.input_rate != 48000 {
        return Err(Error::Configuration);
    }
    let factor = (48000 / config.rate) as i128;
    let packet_start = pts as i128 * factor;
    let packet_duration = duration as i128 * factor;
    if packet_start < 0 {
        if -packet_start != pre_skip as i128 || (skip.start != 0 && skip.start != pre_skip) {
            return Err(Error::Incomplete);
        }
        skip.start = pre_skip;
    }
    let start = packet_start + skip.start as i128;
    let logical_end = packet_start + packet_duration;
    let converted_end = clock.converted_samples as i128 * factor;
    let source_end = clock.samples.status.decoded_samples as i128;
    // Native duration excludes end padding. The last libopus packet may be
    // shorter than the configured frame; legal packet spans are 2.5-ms multiples.
    let coded_span = packet_duration + skip.end as i128;
    if pts == av::AV_NOPTS_VALUE
        || duration <= 0
        || start < 0
        || skip.start as i128 != (-packet_start).max(0)
        || logical_end > converted_end
        || coded_span < 120
        || coded_span > config.frame_ms as i128 * 48
        || coded_span % 120 != 0
    {
        return Err(Error::Incomplete);
    }
    let extra = (logical_end - source_end).max(0);
    if extra > 0 {
        // Only crop the fractional finite resampler endpoint, never arbitrary
        // drift, a pending live packet, missing input, or a whole encoder-rate sample.
        if !clock.locally_finished
            || logical_end != converted_end
            || extra >= factor
            || (skip.end != 0 && skip.end_reason != 0)
        {
            return Err(Error::Incomplete);
        }
        skip.end = skip.end.checked_add(extra as u32).ok_or(Error::Limit)?;
        skip.end_reason = 0; // padding, not convergence
    }
    let end = logical_end - extra;
    if end <= start {
        return Err(Error::Incomplete);
    }
    let origin = clock.first_pts.ok_or(Error::Clock)? as i128;
    let timestamp = |offset: i128| -> Result<i64, Error> {
        i64::try_from(origin + offset)
            .ok()
            .filter(|p| *p != av::AV_NOPTS_VALUE)
            .ok_or(Error::Clock)
    };
    Ok(Presentation {
        packet_timestamp_48k: timestamp(packet_start)?,
        audible_timestamp_48k: timestamp(start)?,
        audible_samples: u32::try_from(end - start).map_err(|_| Error::Clock)?,
        decoder_skip: skip,
        resampler_tail_discard_48k: extra as u32,
    })
}

/// Immutable encoded packet, including FFmpeg skip/discard side data. Holding it
/// retains an output slot and the aggregate processing reservation after shutdown.
/// Data access is cooperative; actual transport writes still need their own fence.
pub struct OpusPacket {
    raw: *mut av::AVPacket,
    job: Arc<Job>,
    born: Instant,
    _slot: OwnedSemaphorePermit,
}
// SAFETY: the packet is private to avcodec_receive_packet until publication, then
// immutable. All buffers/side data are owned; FFmpeg destruction has no affinity.
unsafe impl Send for OpusPacket {}
unsafe impl Sync for OpusPacket {}
impl Drop for OpusPacket {
    fn drop(&mut self) {
        unsafe {
            av::av_packet_free(&mut self.raw);
        }
    }
}
impl OpusPacket {
    fn check(&self) -> Result<(), Error> {
        self.job.check()?;
        if self.born.elapsed() >= self.job.output_residence {
            return Err(Error::Deadline);
        }
        Ok(())
    }
    pub fn data(&self) -> Result<&[u8], Error> {
        self.check()?;
        let packet = unsafe { &*self.raw };
        Ok(unsafe { std::slice::from_raw_parts(packet.data, packet.size as usize) })
    }
    pub fn header(&self) -> Result<&[u8], Error> {
        self.check()?;
        Ok(&self.job.header)
    }
    /// Native encoder side data. Skip/discard counts use config.rate sample units;
    /// a direct 48-kHz Opus decoder needs decoder_skip(), not these counts verbatim.
    pub fn side_data(
        &self,
    ) -> Result<impl Iterator<Item = (av::AVPacketSideDataType, &[u8])>, Error> {
        self.check()?;
        let p = unsafe { &*self.raw };
        let side = if p.side_data_elems == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(p.side_data, p.side_data_elems as usize) }
        };
        Ok(side.iter().map(|s| {
            (
                s.type_,
                if s.size == 0 {
                    &[][..]
                } else {
                    unsafe { std::slice::from_raw_parts(s.data, s.size) }
                },
            )
        }))
    }
    /// Convert codec padding metadata exactly between sample-rate domains. This
    /// does not resample, add/remove source PCM, or repair source clock jitter.
    pub fn decoder_skip(&self) -> Result<Option<DecoderSkip>, Error> {
        for (kind, bytes) in self.side_data()? {
            if kind != av::AVPacketSideDataType::AV_PKT_DATA_SKIP_SAMPLES {
                continue;
            }
            if bytes.len() != 10 {
                return Err(Error::Configuration);
            }
            let factor = 48000 / self.job.config.rate;
            return Ok(Some(DecoderSkip {
                start: u32::from_le_bytes(bytes[..4].try_into().unwrap())
                    .checked_mul(factor)
                    .ok_or(Error::Limit)?,
                end: u32::from_le_bytes(bytes[4..8].try_into().unwrap())
                    .checked_mul(factor)
                    .ok_or(Error::Limit)?,
                start_reason: bytes[8],
                end_reason: bytes[9],
            }));
        }
        Ok(None)
    }
    /// Encoder-private sample ordinal and duration at config.rate. NOT a source
    /// timestamp, and deliberately not interchangeable with ingest::Packet.
    pub fn codec_position(&self) -> Result<(i64, i64), Error> {
        self.check()?;
        let p = unsafe { &*self.raw };
        Ok((p.pts, p.duration))
    }
    /// Canonical live Opus/RFC 7587 coordinate: the complete packet, including
    /// initial codec preroll, at this 48-kHz timestamp. HomeKit's negotiated-rate
    /// wire-clock conversion belongs at the application boundary, not here.
    /// RTP carries no OpusHead/trim command;
    /// receivers decode the preroll rather than silently discarding a first packet.
    /// This API never grants finite tail padding or HSV completion.
    pub fn live_timestamp_48k(&self) -> Result<i64, Error> {
        self.check()?;
        let skip = self.decoder_skip()?.unwrap_or_default();
        let clock = self.job.clock.lock().unwrap_or_else(|p| p.into_inner());
        if clock.samples.policy != ClockPolicy::Presentation {
            return Err(Error::Configuration);
        }
        let p = unsafe { &*self.raw };
        let factor = (48000 / self.job.config.rate) as i128;
        let padding = self.job.initial_padding as i128;
        let header = &self.job.header;
        if header.len() < 19
            || &header[..8] != b"OpusHead"
            || i128::from(u16::from_le_bytes([header[10], header[11]])) != padding * factor
        {
            return Err(Error::Configuration);
        }
        if (p.pts as i128) < -padding
            || p.duration != (self.job.config.rate * self.job.config.frame_ms as u32 / 1000) as i64
            || skip.end != 0
            || (skip.start != 0 && skip.start as i128 != padding * factor)
            || p.pts as i128 + p.duration as i128 > clock.converted_samples as i128
        {
            return Err(Error::Incomplete);
        }
        i64::try_from(
            clock.presentation_origin.ok_or(Error::Clock)? as i128 + p.pts as i128 * factor,
        )
        .map_err(|_| Error::Clock)
    }
    pub fn config(&self) -> Config {
        self.job.config
    }
    pub fn clock_policy(&self) -> ClockPolicy {
        self.job
            .clock
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .samples
            .policy
    }
    /// Candidate priming/tail contract in the Opus decoder's fixed 48-kHz domain,
    /// independent of encoder rate. Not a raw-RTP write grant.
    /// Never throw away a packet needed to establish decoder state, or pretend
    /// its padded samples were captured. Unsupported packet shapes are refused.
    pub fn presentation_48k(&self) -> Result<Presentation, Error> {
        self.check()?;
        let skip = self.decoder_skip()?.unwrap_or_default();
        let clock = self.job.clock.lock().unwrap_or_else(|p| p.into_inner());
        if !matches!(clock.samples.policy, ClockPolicy::AnchoredSamples { .. }) {
            return Err(Error::Configuration);
        }
        clock.samples.ready(Instant::now())?;
        let p = unsafe { &*self.raw };
        let header = &self.job.header;
        if header.len() < 12 || &header[..8] != b"OpusHead" {
            return Err(Error::Configuration);
        }
        let pre_skip = u16::from_le_bytes([header[10], header[11]]) as u32;
        if pre_skip as i128
            != self.job.initial_padding as i128 * (48000 / self.job.config.rate) as i128
        {
            return Err(Error::Incomplete);
        }
        presentation(&clock, self.job.config, p.pts, p.duration, pre_skip, skip)
    }
    /// Native encoder-rate samples; OpusHead separately stores pre-skip at 48 kHz.
    pub fn encoder_initial_padding(&self) -> i32 {
        self.job.initial_padding
    }
    /// Map only fully covered packets. Strict policy requires contiguous source
    /// timestamps; the explicit candidate uses a source-anchored sample clock with
    /// bounded phase/freshness checks. Neither policy grants A/V acceptance.
    /// Priming/tail packets require presentation_48k(), not silently dropped data.
    pub fn source_timestamp_48k(&self) -> Result<i64, Error> {
        self.check()?;
        if self
            .decoder_skip()?
            .is_some_and(|s| s.start != 0 || s.end != 0)
        {
            return Err(Error::Incomplete);
        }
        let clock = self.job.clock.lock().unwrap_or_else(|p| p.into_inner());
        if clock.samples.policy == ClockPolicy::Presentation {
            return Err(Error::Configuration);
        }
        clock.samples.ready(Instant::now())?;
        if clock.samples.policy == ClockPolicy::Strict && clock.mismatch.is_some() {
            return Err(Error::Clock);
        }
        let p = unsafe { &*self.raw };
        if p.pts < 0
            || p.duration <= 0
            || p.pts as i128 + p.duration as i128 > clock.converted_samples as i128
            || (p.pts as i128 + p.duration as i128) * clock.input_rate as i128
                > clock.samples.status.decoded_samples as i128 * self.job.config.rate as i128
        {
            return Err(Error::Incomplete);
        }
        let origin = rescale(
            clock.first_pts.ok_or(Error::Clock)? as i128,
            TimeBase {
                numerator: 1,
                denominator: clock.input_rate as i32,
            },
            TimeBase {
                numerator: 1,
                denominator: 48000,
            },
        )?;
        let offset = rescale(
            p.pts as i128,
            TimeBase {
                numerator: 1,
                denominator: self.job.config.rate as i32,
            },
            TimeBase {
                numerator: 1,
                denominator: 48000,
            },
        )?;
        origin
            .checked_add(offset)
            .filter(|v| *v != av::AV_NOPTS_VALUE)
            .ok_or(Error::Clock)
    }
}

struct Layout(av::AVChannelLayout);
impl Layout {
    fn copy(source: &av::AVChannelLayout) -> Result<Self, Error> {
        // FFmpeg permits the all-zero (unspecified, zero-channel) layout as the
        // destination for channel_layout_copy/uninit.
        let mut layout = Self(unsafe { std::mem::zeroed() });
        native(
            unsafe { av::av_channel_layout_copy(&mut layout.0, source) },
            "copy input layout",
        )?;
        Ok(layout)
    }
}
impl Drop for Layout {
    fn drop(&mut self) {
        unsafe {
            av::av_channel_layout_uninit(&mut self.0);
        }
    }
}
struct CodecContext(*mut av::AVCodecContext);
impl Drop for CodecContext {
    fn drop(&mut self) {
        unsafe {
            av::avcodec_free_context(&mut self.0);
        }
    }
}
struct Frame(*mut av::AVFrame);
impl Frame {
    fn new() -> Result<Self, Error> {
        let p = unsafe { av::av_frame_alloc() };
        if p.is_null() {
            Err(Error::Allocation)
        } else {
            Ok(Self(p))
        }
    }
}
impl Drop for Frame {
    fn drop(&mut self) {
        unsafe {
            av::av_frame_free(&mut self.0);
        }
    }
}
struct Packet(*mut av::AVPacket);
impl Packet {
    fn new() -> Result<Self, Error> {
        let p = unsafe { av::av_packet_alloc() };
        if p.is_null() {
            Err(Error::Allocation)
        } else {
            Ok(Self(p))
        }
    }
}
impl Drop for Packet {
    fn drop(&mut self) {
        unsafe {
            av::av_packet_free(&mut self.0);
        }
    }
}
struct Dictionary(*mut av::AVDictionary);
impl Drop for Dictionary {
    fn drop(&mut self) {
        unsafe {
            av::av_dict_free(&mut self.0);
        }
    }
}
impl Dictionary {
    fn number(&mut self, key: &std::ffi::CStr, value: i64) -> Result<(), Error> {
        native(
            unsafe { av::av_dict_set_int(&mut self.0, key.as_ptr(), value, 0) },
            "audio option",
        )
    }
}
fn native(code: i32, operation: &'static str) -> Result<(), Error> {
    if code < 0 {
        Err(Error::Native { operation, code })
    } else {
        Ok(())
    }
}
fn again(code: i32) -> bool {
    code.checked_neg().is_some_and(|errno| {
        std::io::Error::from_raw_os_error(errno).kind() == std::io::ErrorKind::WouldBlock
    })
}

pub struct Transcoder {
    decoder: CodecContext,
    encoder: CodecContext,
    resampler: *mut av::SwrContext,
    fifo: *mut av::AVAudioFifo,
    job: Arc<Job>,
    stream: usize,
    input_clock: TimeBase,
    input_rate: u32,
    input_layout: Layout,
    next_encoder_sample: i64,
    first_sequence: u64,
    sequence: Option<u64>,
    pending_pts: VecDeque<i64>,
    pending_arrivals: VecDeque<Instant>,
    next_decoder_sample: i64,
    recovery: Option<Recovery>,
    presentation: Option<PresentationClock>,
    startup: Startup,
    presentation_armed: bool,
    stats: Stats,
    finished: bool,
    _thread_bound: PhantomData<Rc<()>>,
}
impl Drop for Transcoder {
    fn drop(&mut self) {
        if !self.finished && self.job.check().is_ok() {
            self.job.fail(Error::Incomplete);
        }
        unsafe {
            av::swr_free(&mut self.resampler);
            av::av_audio_fifo_free(self.fifo);
        }
    }
}
impl Transcoder {
    /// Consume packets from one source-issued audio subscription. The first AAC
    /// packet is not consumed here; push it next. No decoder state/clock guess is
    /// transferred from another generation or an incompatible Config.
    pub fn new(
        first: &Delivery,
        config: Config,
        pool: &Pool,
        cancellation: Cancellation,
    ) -> Result<Self, Error> {
        Self::new_with_clock_policy(first, config, ClockPolicy::Strict, pool, cancellation)
    }
    /// Experimental live policy is explicit and must be part of any sharing key.
    /// Candidate admission waits for an AAC packet carrying a fresh SR and PRFT;
    /// callers may wait for a later packet on the SAME source, never open a reader.
    pub fn new_with_clock_policy(
        first: &Delivery,
        config: Config,
        policy: ClockPolicy,
        pool: &Pool,
        cancellation: Cancellation,
    ) -> Result<Self, Error> {
        Self::new_timed(first, config, policy, None, pool, cancellation, false)
    }
    pub fn new_presented(
        first: &Delivery,
        config: Config,
        clock: PresentationClock,
        pool: &Pool,
        cancellation: Cancellation,
    ) -> Result<Self, Error> {
        Self::new_timed(
            first,
            config,
            ClockPolicy::Presentation,
            Some(clock),
            pool,
            cancellation,
            false,
        )
    }
    fn new_timed(
        first: &Delivery,
        config: Config,
        policy: ClockPolicy,
        presentation: Option<PresentationClock>,
        pool: &Pool,
        cancellation: Cancellation,
        aac: bool,
    ) -> Result<Self, Error> {
        let valid = if aac {
            matches!(config.rate, 32000 | 48000)
                && config.channels == 1
                && (16000..=192000).contains(&config.bitrate)
                && policy == ClockPolicy::Presentation
        } else {
            config.valid()
        };
        if !valid
            || !policy.valid()
            || (policy == ClockPolicy::Presentation) != presentation.is_some()
        {
            return Err(Error::Configuration);
        }
        let permits = vec![
            pool.jobs
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Limit)?,
            pool.bytes
                .clone()
                .try_acquire_many_owned(if aac {
                    AAC_RESERVATION_BYTES as u32
                } else {
                    RESERVATION as u32
                })
                .map_err(|_| Error::Limit)?,
        ];
        if (first.timing()? == crate::source::Timing::Presentation) != presentation.is_some() {
            return Err(Error::Configuration);
        }
        let packet = first.packet()?;
        if matches!(policy, ClockPolicy::AnchoredSamples { .. })
            && (packet.sender_report().is_none()
                || packet.producer_reference().is_none_or(|r| r.flags != 24))
        {
            return Err(Error::Incomplete);
        }
        let stream = &first.configuration()?[packet.stream_index()];
        let parameters = unsafe { &*stream.parameters.as_ptr() };
        if stream.codec != Codec::Aac
            || stream.parameters.extradata().len() < 2
            || stream.parameters.extradata()[0] >> 3 != 2
            || !(8000..=96000).contains(&parameters.sample_rate)
            || !(1..=2).contains(&parameters.ch_layout.nb_channels)
        {
            return Err(Error::Configuration);
        }
        if matches!(policy, ClockPolicy::AnchoredSamples { .. }) && parameters.sample_rate != 48000
        {
            return Err(Error::Configuration);
        }
        let input_layout = Layout::copy(&parameters.ch_layout)?;
        let decoder_codec = unsafe { av::avcodec_find_decoder(av::AVCodecID::AV_CODEC_ID_AAC) };
        let encoder_codec = unsafe {
            av::avcodec_find_encoder_by_name(if aac {
                c"aac".as_ptr()
            } else {
                c"libopus".as_ptr()
            })
        };
        if decoder_codec.is_null() || encoder_codec.is_null() {
            return Err(Error::Configuration);
        }
        let decoder = CodecContext(unsafe { av::avcodec_alloc_context3(decoder_codec) });
        let encoder = CodecContext(unsafe { av::avcodec_alloc_context3(encoder_codec) });
        if decoder.0.is_null() || encoder.0.is_null() {
            return Err(Error::Allocation);
        }
        native(
            unsafe { av::avcodec_parameters_to_context(decoder.0, stream.parameters.as_ptr()) },
            "AAC parameters",
        )?;
        unsafe {
            (*decoder.0).pkt_timebase = if presentation.is_some() {
                av::AVRational {
                    num: 1,
                    den: parameters.sample_rate,
                }
            } else {
                av::AVRational {
                    num: stream.time_base.numerator,
                    den: stream.time_base.denominator,
                }
            };
            (*decoder.0).thread_count = 1;
        }
        native(
            unsafe { av::avcodec_open2(decoder.0, decoder_codec, ptr::null_mut()) },
            "open AAC decoder",
        )?;
        unsafe {
            (*encoder.0).sample_rate = config.rate as i32;
            if aac {
                (*encoder.0).profile = av::AV_PROFILE_AAC_LOW;
                (*encoder.0).flags |= av::AV_CODEC_FLAG_GLOBAL_HEADER as i32;
            }
            (*encoder.0).sample_fmt = if aac {
                av::AVSampleFormat::AV_SAMPLE_FMT_FLTP
            } else {
                av::AVSampleFormat::AV_SAMPLE_FMT_S16
            };
            (*encoder.0).time_base = av::AVRational {
                num: 1,
                den: config.rate as i32,
            };
            (*encoder.0).bit_rate = config.bitrate as i64;
            (*encoder.0).thread_count = 1;
            av::av_channel_layout_default(&mut (*encoder.0).ch_layout, config.channels as i32);
        }
        let mut options = Dictionary(ptr::null_mut());
        if !aac {
            native(
                unsafe {
                    av::av_dict_set(&mut options.0, c"application".as_ptr(), c"voip".as_ptr(), 0)
                },
                "Opus application",
            )?;
            options.number(c"frame_duration", config.frame_ms as i64)?;
            options.number(c"vbr", i64::from(config.vbr))?;
            options.number(c"fec", i64::from(config.fec))?;
            options.number(c"packet_loss", config.expected_loss_percent as i64)?;
        }
        native(
            unsafe { av::avcodec_open2(encoder.0, encoder_codec, &mut options.0) },
            "open audio encoder",
        )?;
        if !options.0.is_null() {
            return Err(Error::Configuration);
        }
        let enc = unsafe { &*encoder.0 };
        if enc.frame_size
            != if aac {
                1024
            } else {
                (config.rate * config.frame_ms as u32 / 1000) as i32
            }
            || enc.extradata.is_null()
            || enc.extradata_size <= 0
            || enc.extradata_size > 4096
            || unsafe {
                (*encoder_codec).capabilities as u32 & av::AV_CODEC_CAP_SMALL_LAST_FRAME == 0
            }
        {
            return Err(Error::Configuration);
        }
        let header =
            unsafe { std::slice::from_raw_parts(enc.extradata, enc.extradata_size as usize) }
                .to_vec();
        let job = Arc::new(Job {
            source: first.guard()?,
            cancellation,
            failure: Mutex::new(None),
            clock: Mutex::new(Clock {
                presentation_origin: None,
                first_pts: None,
                previous_end: None,
                mismatch: None,
                input_rate: parameters.sample_rate as u32,
                converted_samples: 0,
                locally_finished: false,
                samples: SampleClock::new(policy, Instant::now()),
            }),
            header,
            config,
            initial_padding: enc.initial_padding,
            output_residence: Duration::from_secs(if aac { 16 } else { 2 }),
            outputs: Arc::new(Semaphore::new(if aac { 1024 } else { 64 })),
            _permits: permits,
        });
        job.check()?;
        let fifo = unsafe {
            av::av_audio_fifo_alloc(enc.sample_fmt, config.channels as i32, FIFO_SAMPLES)
        };
        if fifo.is_null() {
            return Err(Error::Allocation);
        }
        Ok(Self {
            decoder,
            encoder,
            resampler: ptr::null_mut(),
            fifo,
            job,
            stream: stream.index,
            input_clock: if presentation.is_some() {
                TimeBase {
                    numerator: 1,
                    denominator: parameters.sample_rate,
                }
            } else {
                stream.time_base
            },
            input_rate: parameters.sample_rate as u32,
            input_layout,
            next_encoder_sample: 0,
            first_sequence: packet.sequence(),
            sequence: None,
            pending_pts: VecDeque::new(),
            pending_arrivals: VecDeque::new(),
            next_decoder_sample: 0,
            recovery: presentation.map(|_| Recovery::new(packet.received_at())),
            presentation,
            startup: Startup::new(packet.received_at()),
            presentation_armed: false,
            stats: Stats::default(),
            finished: false,
            _thread_bound: PhantomData,
        })
    }
    pub fn recovery_status(&self) -> Option<RecoveryStatus> {
        self.recovery.as_ref().map(Recovery::status)
    }
    pub fn clock_status(&self) -> ClockStatus {
        self.job
            .clock
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .samples
            .snapshot(Instant::now())
    }
    pub fn stats(&self) -> Stats {
        self.stats
    }
    pub fn push(&mut self, delivery: &Delivery) -> Result<Vec<OpusPacket>, Error> {
        if self.finished {
            return Err(Error::Closed);
        }
        let result = self.push_inner(delivery);
        result.map_err(|e| self.job.fail(e))
    }
    fn push_inner(&mut self, delivery: &Delivery) -> Result<Vec<OpusPacket>, Error> {
        self.job.check()?;
        if self.finished {
            return Err(Error::Closed);
        }
        self.job.source.accepts(delivery)?;
        let packet = delivery.packet()?;
        if packet.stream_index() != self.stream
            || packet.data().len() > MAX_PACKET_BYTES
            || (self.sequence.is_none() && packet.sequence() != self.first_sequence)
            || self
                .sequence
                .is_some_and(|previous| packet.sequence() <= previous)
        {
            return Err(Error::Order);
        }
        if self.pending_pts.len() >= 8 {
            return Err(Error::Limit);
        }
        if self.recovery.is_none()
            && matches!(
                self.job
                    .clock
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .samples
                    .policy,
                ClockPolicy::AnchoredSamples { .. }
            )
            && packet.producer_reference().is_none_or(|r| r.flags != 24)
        {
            return Err(Error::Clock);
        }
        let pts = if self.recovery.is_some() {
            self.next_decoder_sample
        } else {
            packet.pts().ok_or(Error::Clock)?
        };
        self.pending_arrivals.push_back(packet.received_at());
        self.pending_pts.push_back(pts);
        self.sequence = Some(packet.sequence());
        self.stats.input_packets += 1;
        if let Some(sr) = packet.sender_report() {
            self.stats.sender_reports += 1;
            if self.stats.last_sender_report != Some(sr) {
                self.job
                    .clock
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .samples
                    .report(packet.received_at());
            }
            self.stats.last_sender_report = Some(sr);
        }
        if self.recovery.is_some() {
            // A private decoder copy, never a rewrite of the shared source packet.
            let copy = Packet::new()?;
            native(
                unsafe { av::av_packet_ref(copy.0, packet.as_ptr()) },
                "copy AAC",
            )?;
            unsafe {
                (*copy.0).pts = pts;
                (*copy.0).dts = pts;
                (*copy.0).duration = 1024;
            }
            self.next_decoder_sample = self
                .next_decoder_sample
                .checked_add(1024)
                .ok_or(Error::Limit)?;
            native(
                unsafe { av::avcodec_send_packet(self.decoder.0, copy.0) },
                "send AAC",
            )?;
        } else {
            native(
                unsafe { av::avcodec_send_packet(self.decoder.0, packet.as_ptr()) },
                "send AAC",
            )?;
        }
        let mut output = Vec::new();
        self.decode(&mut output, false)?;
        Ok(output)
    }
    fn decode(&mut self, output: &mut Vec<OpusPacket>, draining: bool) -> Result<(), Error> {
        loop {
            let frame = Frame::new()?;
            let code = unsafe { av::avcodec_receive_frame(self.decoder.0, frame.0) };
            if code == av::AVERROR_EOF {
                return if draining {
                    Ok(())
                } else {
                    Err(Error::Incomplete)
                };
            }
            if again(code) {
                return if draining {
                    Err(Error::Incomplete)
                } else {
                    Ok(())
                };
            }
            native(code, "receive AAC frame")?;
            let f = unsafe { &*frame.0 };
            if f.format != av::AVSampleFormat::AV_SAMPLE_FMT_FLTP as i32
                || f.sample_rate != self.input_rate as i32
                || f.nb_samples <= 0
                || f.nb_samples > 2048
                || unsafe { av::av_channel_layout_compare(&f.ch_layout, &self.input_layout.0) != 0 }
                || self.pending_pts.pop_front() != Some(f.pts)
            {
                return Err(Error::Configuration);
            }
            let arrived = self.pending_arrivals.pop_front().ok_or(Error::Order)?;
            if self.recovery.is_some() && f.nb_samples != 1024 {
                return Err(Error::Configuration);
            }
            self.stats.decoded_samples += f.nb_samples as u64;
            if self.recovery.is_some() && !self.presentation_armed {
                // Decode probe backlog/one AAC history packet before choosing the
                // single live anchor. Never change that anchor after output starts.
                if !self
                    .startup
                    .admit(arrived, self.input_rate, f.nb_samples as u32)?
                {
                    self.stats.preroll_samples += f.nb_samples as u64;
                    continue;
                }
                self.presentation_armed = true;
                self.recovery = Some(Recovery::new(arrived));
                self.job
                    .clock
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .presentation_origin = Some(
                    i64::try_from(self.presentation.ok_or(Error::Clock)?.ticks(arrived, 48000))
                        .map_err(|_| Error::Clock)?,
                );
            }
            let sample_pts = rescale(
                f.pts as i128,
                self.input_clock,
                TimeBase {
                    numerator: 1,
                    denominator: self.input_rate as i32,
                },
            )?;
            {
                let mut clock = self.job.clock.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(expected) = clock.previous_end
                    && expected != sample_pts
                {
                    let mismatch = ClockMismatch {
                        expected,
                        actual: sample_pts,
                    };
                    clock.mismatch.get_or_insert(mismatch);
                    self.stats.first_mismatch.get_or_insert(mismatch);
                    self.stats.clock_discontinuities += 1;
                    let error = sample_pts as i128 - expected as i128;
                    self.stats.minimum_step_error = Some(
                        self.stats
                            .minimum_step_error
                            .map_or(error, |old| old.min(error)),
                    );
                    self.stats.maximum_step_error = Some(
                        self.stats
                            .maximum_step_error
                            .map_or(error, |old| old.max(error)),
                    );
                }
                clock.samples.frame(sample_pts, f.nb_samples as u32)?;
                clock.first_pts.get_or_insert(sample_pts);
                clock.previous_end = Some(
                    sample_pts
                        .checked_add(f.nb_samples as i64)
                        .ok_or(Error::Clock)?,
                );
            }
            if self.resampler.is_null() {
                native(
                    unsafe {
                        av::swr_alloc_set_opts2(
                            &mut self.resampler,
                            &(*self.encoder.0).ch_layout,
                            (*self.encoder.0).sample_fmt,
                            self.job.config.rate as i32,
                            &f.ch_layout,
                            av::AVSampleFormat::AV_SAMPLE_FMT_FLTP,
                            f.sample_rate,
                            0,
                            ptr::null_mut(),
                        )
                    },
                    "allocate resampler",
                )?;
                native(
                    unsafe { av::swr_init(self.resampler) },
                    "initialize resampler",
                )?;
            }
            if let Some(recovery) = &mut self.recovery {
                let rate = self.job.config.rate;
                let delayed = unsafe { av::swr_get_delay(self.resampler, rate as i64) };
                if delayed < 0 {
                    return Err(Error::Clock);
                }
                let projected = (self.stats.resampled_samples as i128 + delayed as i128) * 48000
                    / i128::from(rate);
                let delta = recovery.step(arrived, projected, rate)?;
                native(
                    unsafe { av::swr_set_compensation(self.resampler, delta, rate as i32) },
                    "audio clock compensation",
                )?;
            }
            self.convert(f.extended_data.cast(), f.nb_samples)?;
            self.encode_ready(output, false)?;
        }
    }
    fn audio_frame(&self, samples: i32) -> Result<Frame, Error> {
        let frame = Frame::new()?;
        unsafe {
            (*frame.0).format = (*self.encoder.0).sample_fmt as i32;
            (*frame.0).sample_rate = self.job.config.rate as i32;
            (*frame.0).nb_samples = samples;
            native(
                av::av_channel_layout_copy(&mut (*frame.0).ch_layout, &(*self.encoder.0).ch_layout),
                "frame channels",
            )?;
            native(av::av_frame_get_buffer(frame.0, 0), "allocate audio frame")?;
        }
        Ok(frame)
    }
    fn convert(&mut self, input: *const *const u8, samples: i32) -> Result<i32, Error> {
        let capacity = unsafe { av::swr_get_out_samples(self.resampler, samples) };
        if capacity < 0 {
            return native(capacity, "resample capacity").map(|_| 0);
        }
        if capacity > CONVERT_SAMPLES {
            return Err(Error::Limit);
        }
        let converted = self.audio_frame(capacity.max(1))?;
        let count = unsafe {
            av::swr_convert(
                self.resampler,
                (*converted.0).data.as_mut_ptr(),
                capacity,
                input,
                samples,
            )
        };
        native(count, "resample AAC")?;
        if unsafe { av::av_audio_fifo_size(self.fifo) } + count > FIFO_SAMPLES {
            return Err(Error::Limit);
        }
        if count > 0 {
            let written = unsafe {
                av::av_audio_fifo_write(self.fifo, (*converted.0).data.as_mut_ptr().cast(), count)
            };
            if written != count {
                return Err(Error::Allocation);
            }
            self.stats.resampled_samples += count as u64;
            self.job
                .clock
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .converted_samples = self.stats.resampled_samples;
        }
        Ok(count)
    }
    fn encode_ready(&mut self, output: &mut Vec<OpusPacket>, tail: bool) -> Result<(), Error> {
        let frame_size = unsafe { (*self.encoder.0).frame_size };
        loop {
            let available = unsafe { av::av_audio_fifo_size(self.fifo) };
            if available == 0 || (!tail && available < frame_size) {
                return Ok(());
            }
            let count = available.min(frame_size);
            let frame = self.audio_frame(count)?;
            let read = unsafe {
                av::av_audio_fifo_read(self.fifo, (*frame.0).data.as_mut_ptr().cast(), count)
            };
            if read != count {
                return Err(Error::Incomplete);
            }
            // Private encoder sample ordinal, never a rewritten source PTS.
            // Presentation compensation has already been applied by the resampler.
            unsafe {
                (*frame.0).pts = self.next_encoder_sample;
            }
            self.next_encoder_sample = self
                .next_encoder_sample
                .checked_add(count as i64)
                .ok_or(Error::Limit)?;
            native(
                unsafe { av::avcodec_send_frame(self.encoder.0, frame.0) },
                "send audio frame",
            )?;
            self.receive(output, false)?;
        }
    }
    fn receive(&mut self, output: &mut Vec<OpusPacket>, draining: bool) -> Result<(), Error> {
        loop {
            let mut packet = Packet::new()?;
            let code = unsafe { av::avcodec_receive_packet(self.encoder.0, packet.0) };
            if code == av::AVERROR_EOF {
                return if draining {
                    Ok(())
                } else {
                    Err(Error::Incomplete)
                };
            }
            if again(code) {
                return if draining {
                    Err(Error::Incomplete)
                } else {
                    Ok(())
                };
            }
            native(code, "receive audio packet")?;
            let p = unsafe { &*packet.0 };
            if p.size <= 0
                || p.size as usize > MAX_PACKET_BYTES
                || p.pts == av::AV_NOPTS_VALUE
                || p.duration <= 0
            {
                return Err(Error::Configuration);
            }
            if output.len() >= 64 {
                return Err(Error::Limit);
            }
            let slot = self
                .job
                .outputs
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Limit)?;
            self.stats.encoded_packets += 1;
            self.stats.payload_bytes += p.size as u64;
            self.job.check()?;
            output.push(OpusPacket {
                raw: packet.0,
                job: self.job.clone(),
                born: Instant::now(),
                _slot: slot,
            });
            packet.0 = ptr::null_mut(); // ownership transferred exactly once
        }
    }
    /// Finite local codec completion only. Decoder/resampler/encoder tails are
    /// flushed explicitly; libopus padding/skip metadata is retained. Camera EOF
    /// and cancellation must retire the owner instead, never call finish implicitly.
    pub fn finish(&mut self) -> Result<Vec<OpusPacket>, Error> {
        if self.finished {
            return Err(Error::Closed);
        }
        let result = self.finish_inner();
        result.map_err(|e| self.job.fail(e))
    }
    fn finish_inner(&mut self) -> Result<Vec<OpusPacket>, Error> {
        self.job.check()?;
        if self.recovery.is_some() {
            return Err(Error::Configuration);
        }
        if self.finished {
            return Err(Error::Closed);
        }
        let mut output = Vec::new();
        native(
            unsafe { av::avcodec_send_packet(self.decoder.0, ptr::null()) },
            "flush AAC",
        )?;
        self.decode(&mut output, true)?;
        if !self.pending_pts.is_empty() || self.resampler.is_null() {
            return Err(Error::Incomplete);
        }
        // swr_get_out_samples is bounded above; flushing cannot inject arbitrary
        // silence or compensate timestamps. Drain only the filter's retained tail.
        let mut drained = false;
        for _ in 0..8 {
            let count = self.convert(ptr::null(), 0)?;
            self.encode_ready(&mut output, false)?;
            if count == 0 {
                drained = true;
                break;
            }
        }
        if !drained {
            return Err(Error::Incomplete);
        }
        self.encode_ready(&mut output, true)?;
        native(
            unsafe { av::avcodec_send_frame(self.encoder.0, ptr::null()) },
            "flush audio encoder",
        )?;
        self.receive(&mut output, true)?;
        {
            let mut clock = self.job.clock.lock().unwrap_or_else(|p| p.into_inner());
            if clock.samples.policy != ClockPolicy::Strict {
                finite_coverage(&clock, self.job.config)?;
            }
            clock.locally_finished = true;
        }
        self.finished = true;
        Ok(output)
    }
}

#[cfg(test)]
mod presentation_tests {
    use super::*;
    fn clock(rate: u32, decoded: u64, finished: bool) -> Clock {
        let mut samples = SampleClock::new(ClockPolicy::candidate(), Instant::now());
        samples.status.decoded_samples = decoded;
        Clock {
            presentation_origin: None,
            first_pts: Some(-16),
            previous_end: None,
            mismatch: None,
            input_rate: 48000,
            converted_samples: decoded.div_ceil((48000 / rate) as u64),
            locally_finished: finished,
            samples,
        }
    }
    #[test]
    fn preskip_and_signed_origin_are_in_the_decoder_domain() {
        for rate in [8000, 16000, 24000, 48000] {
            let factor = 48000 / rate;
            let config = Config {
                rate,
                ..Default::default()
            };
            let p = presentation(
                &clock(rate, 1024, false),
                config,
                -312 / factor as i64,
                960 / factor as i64,
                312,
                DecoderSkip::default(),
            )
            .unwrap();
            assert_eq!(p.packet_timestamp_48k, -328);
            assert_eq!(p.audible_timestamp_48k, -16);
            assert_eq!(p.audible_samples, 648);
            assert_eq!(p.decoder_skip.start, 312);
            assert_eq!(p.resampler_tail_discard_48k, 0);
        }
    }
    #[test]
    fn finite_rounding_is_an_exact_explicit_end_trim() {
        for rate in [8000, 16000, 24000, 48000] {
            for packets in [199, 200, 201] {
                let c = clock(rate, packets * 1024, true);
                let factor = 48000 / rate;
                let extra = c.converted_samples * factor as u64 - packets * 1024;
                let config = Config {
                    rate,
                    ..Default::default()
                };
                finite_coverage(&c, config).unwrap();
                let native = DecoderSkip {
                    end: 960 - factor,
                    ..Default::default()
                };
                let p = presentation(&c, config, c.converted_samples as i64 - 1, 1, 312, native)
                    .unwrap();
                assert_eq!(p.resampler_tail_discard_48k, extra as u32);
                assert_eq!(p.decoder_skip.end, native.end + extra as u32);
                assert_eq!(
                    p.audible_timestamp_48k + p.audible_samples as i64,
                    -16 + packets as i64 * 1024
                );
            }
        }
    }
    #[test]
    fn live_or_uncovered_endpoints_are_not_cropped_to_fit() {
        let config = Config {
            rate: 8000,
            ..Default::default()
        };
        let native = DecoderSkip {
            end: 954,
            ..Default::default()
        };
        let mut c = clock(8000, 1024, false);
        assert_eq!(
            presentation(&c, config, 170, 1, 312, native),
            Err(Error::Incomplete)
        );
        c.locally_finished = true;
        c.converted_samples = 170; // undercoverage
        assert_eq!(finite_coverage(&c, config), Err(Error::Incomplete));
        c.converted_samples = 172; // more than a fractional endpoint
        assert_eq!(finite_coverage(&c, config), Err(Error::Incomplete));
        assert_eq!(
            presentation(&c, config, 171, 1, 312, native),
            Err(Error::Incomplete)
        );
        assert_eq!(
            presentation(&c, config, 170, 1, 312, native),
            Err(Error::Incomplete)
        );
    }
    #[test]
    fn shorter_final_opus_packet_is_valid_but_invalid_spans_are_refused() {
        let config = Config {
            rate: 16000,
            frame_ms: 60,
            ..Default::default()
        };
        let c = clock(16000, 1024, true);
        let skip = DecoderSkip {
            end: 117,
            ..Default::default()
        };
        // 2.5-ms final coded packet, one encoder sample of valid duration.
        assert!(presentation(&c, config, 341, 1, 312, skip).is_ok());
        assert_eq!(
            presentation(&c, config, 341, 1, 312, DecoderSkip::default()),
            Err(Error::Incomplete)
        );
        assert_eq!(
            presentation(&c, config, av::AV_NOPTS_VALUE, 1, 312, skip),
            Err(Error::Incomplete)
        );
    }
}
