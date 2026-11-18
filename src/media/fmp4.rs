//! Finite, generation-bound packet-copy fMP4 muxing through public FFmpeg APIs.
//! All native state is thread-bound. Use on a blocking media worker, not the async
//! executor. No files, worker processes or camera URLs are opened here.
//!
//! One explicit common origin is subtracted exactly from every selected track.
//! This is NOT a decision about covering AAC history, decoder preroll or end trim.
//! Source packet durations are passed unchanged, but MP4 decode sample durations
//! are derived by FFmpeg from consecutive DTS where those disagree. Real-camera
//! evidence records that disagreement; this layer does not claim duration equality
//! or production presentation acceptance.
//! Finish means local mux completion, never source EOF or confirmed Home recording.
use super::{Error, audio::AacPacket, rescale};
use crate::{
    ingest::{Cancellation, Codec, TimeBase},
    source::{Delivery, DeliveryGuard, nals},
};
use ffmpeg_sys_next as av;
use std::{
    ffi::c_void,
    marker::PhantomData,
    ptr,
    rc::Rc,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Aggregate admission for concurrent mux jobs AND their escaped output chunks.
/// A job reserves its full finite input/output allowance before allocating a muxer.
#[derive(Clone)]
pub struct Pool {
    jobs: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    capacity: usize,
    job_limit: usize,
}
impl Pool {
    pub fn new(jobs: usize, bytes: usize) -> Result<Self, Error> {
        if jobs == 0 || jobs > 64 || bytes == 0 || bytes > u32::MAX as usize {
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

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub packets: usize,
    pub chunks: usize,
    pub lifetime: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            input_bytes: 32 * 1024 * 1024,
            output_bytes: 32 * 1024 * 1024,
            packets: 8192,
            chunks: 8192,
            lifetime: Duration::from_secs(60),
        }
    }
}
struct Job {
    source: DeliveryGuard,
    cancellation: Cancellation,
    deadline: Instant,
    failure: Mutex<Option<Error>>,
    retired: tokio::sync::watch::Sender<bool>,
    _permits: Vec<OwnedSemaphorePermit>,
}
impl Job {
    fn fail(&self, error: Error) -> Error {
        let failure = *self
            .failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_or_insert(error);
        self.retired.send_replace(true);
        failure
    }
    fn check(&self) -> Result<(), Error> {
        if let Some(error) = *self.failure.lock().unwrap_or_else(|p| p.into_inner()) {
            return Err(error);
        }
        let result = if self.cancellation.is_cancelled() {
            Err(Error::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(Error::Deadline)
        } else {
            self.source.check().map_err(Error::from)
        };
        result.map_err(|error| self.fail(error))
    }
}

/// Owns copied output and its aggregate reservation. Not Clone; source/privacy,
/// job cancellation and lifetime are rechecked before borrowing bytes. Already
/// borrowed or copied bytes still require a fence at the actual transport write.
pub struct Chunk {
    bytes: Vec<u8>,
    job: Arc<Job>,
}
/// Revocable output permission retained with copied/encrypted recording bytes.
#[derive(Clone)]
pub struct WriteGuard(Arc<Job>);
impl WriteGuard {
    pub fn current(&self) -> bool {
        self.0.check().is_ok()
    }
    /// Hold only across a nonblocking syscall, never producer work or an await.
    pub fn write<T>(&self, syscall: impl FnOnce() -> T) -> Result<T, Error> {
        self.0.check()?;
        let failure = self.0.failure.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(error) = *failure {
            return Err(error);
        }
        if self.0.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if Instant::now() >= self.0.deadline {
            return Err(Error::Deadline);
        }
        self.0.source.with_current(syscall).map_err(Error::from)
    }
    pub async fn retired(&self) {
        let mut closed = self.0.retired.subscribe();
        if self.0.check().is_err() {
            return;
        }
        tokio::select! {
            _ = self.0.source.retired() => (),
            _ = closed.changed() => (),
            _ = tokio::time::sleep_until(self.0.deadline.into()) => (),
        }
    }
}
impl Chunk {
    pub fn guard(&self) -> WriteGuard {
        WriteGuard(self.job.clone())
    }
    pub fn bytes(&self) -> Result<&[u8], Error> {
        self.job.check()?;
        Ok(&self.bytes)
    }
}
struct Output {
    pending: Vec<u8>,
    total: usize,
    maximum: usize,
    job: Arc<Job>,
}
impl Output {
    fn append(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.job.check()?;
        let total = self.total.checked_add(bytes.len()).ok_or(Error::Limit)?;
        if total > self.maximum {
            return Err(Error::Limit);
        }
        self.pending
            .try_reserve_exact(bytes.len())
            .map_err(|_| Error::Allocation)?;
        self.pending.extend_from_slice(bytes);
        self.total = total;
        Ok(())
    }
}
// SAFETY: opaque is the muxer's stable Box<Output>, alive until AFTER AVIO is
// freed. FFmpeg invokes this synchronously on the owning thread. No Rust unwind
// may cross the callback; all failures are sticky in Job and returned to FFmpeg.
unsafe extern "C" fn write(opaque: *mut c_void, data: *const u8, size: i32) -> i32 {
    let output = unsafe { &mut *opaque.cast::<Output>() };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if size < 0 || (size > 0 && data.is_null()) {
            return Err(Error::Configuration);
        }
        let bytes = if size == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(data, size as usize) }
        };
        output.append(bytes)
    }));
    match result {
        Ok(Ok(())) => size,
        Ok(Err(error)) => {
            output.job.fail(error);
            av::AVERROR_EXTERNAL
        }
        Err(_) => {
            output.job.fail(Error::Allocation);
            av::AVERROR_EXTERNAL
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
    fn set(&mut self, key: &std::ffi::CStr, value: &std::ffi::CStr) -> Result<(), Error> {
        native(
            unsafe { av::av_dict_set(&mut self.0, key.as_ptr(), value.as_ptr(), 0) },
            "mux options",
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
struct MutablePacket(*mut av::AVPacket);
impl Drop for MutablePacket {
    fn drop(&mut self) {
        unsafe {
            av::av_packet_free(&mut self.0);
        }
    }
}
struct Track {
    output: usize,
    input_clock: TimeBase,
    shift: i64,
    last_dts: Option<i64>,
    started: bool,
}

pub struct Muxer {
    format: *mut av::AVFormatContext,
    io: *mut av::AVIOContext,
    output: Box<Output>,
    tracks: Vec<Option<Track>>,
    job: Arc<Job>,
    limits: Limits,
    input_bytes: usize,
    packets: usize,
    first_sequence: u64,
    sequence: Option<u64>,
    chunks: usize,
    finished: bool,
    presented: bool,
    audio_parameters: Option<Arc<crate::ingest::CodecParameters>>,
    _thread_bound: PhantomData<Rc<()>>,
}
impl Drop for Muxer {
    fn drop(&mut self) {
        // Failure/abandonment is never implicit successful trailer completion.
        if !self.finished {
            self.job.fail(Error::Incomplete);
        }
        unsafe {
            if !self.format.is_null() {
                (*self.format).pb = ptr::null_mut();
                av::avformat_free_context(self.format);
            }
            if !self.io.is_null() {
                // avio_alloc_context may replace the buffer; free the current one.
                av::av_free((*self.io).buffer.cast());
                (*self.io).buffer = ptr::null_mut();
                av::avio_context_free(&mut self.io);
            }
        }
    }
}
impl Muxer {
    /// Uses the first admitted video's PTS as an exact COMMON origin. Audio can
    /// consequently remain negative. No per-track zeroing or packet-order clocks.
    /// The first packet is not consumed: call push(first) after construction.
    pub fn new(
        first: &Delivery,
        audio: bool,
        pool: &Pool,
        limits: Limits,
        cancellation: Cancellation,
    ) -> Result<Self, Error> {
        Self::new_inner(first, audio, pool, limits, cancellation, None, None)
    }
    /// Clip-local muxing of recovered recording media. The IDR's normalized video
    /// coordinate is the common origin; covering AAC preroll may remain negative.
    pub fn new_presented(
        first: &Delivery,
        video_origin: i64,
        audio: Option<&AacPacket>,
        pool: &Pool,
        limits: Limits,
        cancellation: Cancellation,
    ) -> Result<Self, Error> {
        Self::new_inner(
            first,
            audio.is_some(),
            pool,
            limits,
            cancellation,
            Some(video_origin),
            audio,
        )
    }
    fn new_inner(
        first: &Delivery,
        audio: bool,
        pool: &Pool,
        limits: Limits,
        cancellation: Cancellation,
        presented_origin: Option<i64>,
        encoded_audio: Option<&AacPacket>,
    ) -> Result<Self, Error> {
        if limits.input_bytes == 0
            || limits.output_bytes == 0
            || limits.packets == 0
            || limits.chunks == 0
            || limits.chunks > 65536
            || limits.lifetime.is_zero()
            || limits.lifetime > Duration::from_secs(300)
        {
            return Err(Error::Configuration);
        }
        let reserve = limits
            .input_bytes
            .checked_add(limits.output_bytes)
            .and_then(|n| n.checked_add(128 * 1024 + limits.chunks * 128))
            .and_then(|n| u32::try_from(n).ok())
            .ok_or(Error::Limit)?;
        let permits = vec![
            pool.jobs
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Limit)?,
            pool.bytes
                .clone()
                .try_acquire_many_owned(reserve)
                .map_err(|_| Error::Limit)?,
        ];
        if (first.timing()? == crate::source::Timing::Presentation) != presented_origin.is_some() {
            return Err(Error::Configuration);
        }
        let packet = first.packet()?;
        let streams = first.configuration()?;
        let video = &streams[packet.stream_index()];
        if video.codec != Codec::H264 || !nals(packet.data())?.iter().any(|nal| nal[0] & 31 == 5) {
            return Err(Error::Configuration);
        }
        if audio && !streams.iter().any(|s| s.codec == Codec::Aac) {
            return Err(Error::Configuration);
        }
        let origin = match presented_origin {
            Some(origin) => origin,
            None => packet.pts().ok_or(Error::Clock)?,
        };
        let job = Arc::new(Job {
            source: first.guard()?,
            cancellation,
            deadline: Instant::now() + limits.lifetime,
            failure: Mutex::new(None),
            retired: tokio::sync::watch::channel(false).0,
            _permits: permits,
        });
        job.check()?;
        if let Some(audio) = encoded_audio {
            job.source.accepts_guard(audio.guard())?;
        }
        let mut muxer = Self {
            format: ptr::null_mut(),
            io: ptr::null_mut(),
            output: Box::new(Output {
                pending: Vec::new(),
                total: 0,
                maximum: limits.output_bytes,
                job: job.clone(),
            }),
            tracks: (0..streams.len()).map(|_| None).collect(),
            job,
            limits,
            input_bytes: 0,
            packets: 0,
            first_sequence: packet.sequence(),
            sequence: None,
            chunks: 0,
            finished: false,
            presented: presented_origin.is_some(),
            audio_parameters: encoded_audio.map(|packet| packet.parameters().clone()),
            _thread_bound: PhantomData,
        };
        native(
            unsafe {
                av::avformat_alloc_output_context2(
                    &mut muxer.format,
                    ptr::null_mut(),
                    c"mp4".as_ptr(),
                    ptr::null(),
                )
            },
            "allocate muxer",
        )?;
        if muxer.format.is_null() {
            return Err(Error::Allocation);
        }
        let mut timescale = 1i64;
        // Deterministic output layout: H.264 then optional AAC, independent of
        // the camera's input stream order. All parameter fields/side data are copied.
        for codec in [Codec::H264, Codec::Aac] {
            if codec == Codec::Aac && !audio {
                continue;
            }
            let source = streams
                .iter()
                .find(|s| s.codec == codec)
                .ok_or(Error::Configuration)?;
            let stream = unsafe { av::avformat_new_stream(muxer.format, ptr::null()) };
            if stream.is_null() {
                return Err(Error::Allocation);
            }
            let parameters = if codec == Codec::Aac {
                encoded_audio
                    .map(|a| a.parameters())
                    .unwrap_or(&source.parameters)
            } else {
                &source.parameters
            };
            let input_clock = if presented_origin.is_some() {
                TimeBase {
                    numerator: 1,
                    denominator: if codec == Codec::H264 {
                        90000
                    } else {
                        parameters.audio_format().0
                    },
                }
            } else {
                source.time_base
            };
            native(
                unsafe { av::avcodec_parameters_copy((*stream).codecpar, parameters.as_ptr()) },
                "copy codec parameters",
            )?;
            unsafe {
                (*(*stream).codecpar).codec_tag = 0;
                (*stream).time_base = av::AVRational {
                    num: input_clock.numerator,
                    den: input_clock.denominator,
                };
            }
            let denominator = input_clock.denominator as i64;
            let gcd = unsafe { av::av_gcd(timescale, denominator) };
            timescale = timescale
                .checked_mul(denominator / gcd)
                .filter(|v| *v <= i32::MAX as i64)
                .ok_or(Error::Clock)?;
            let shift = if presented_origin.is_some() {
                i64::try_from(
                    (i128::from(origin) * i128::from(input_clock.denominator)).div_euclid(90000),
                )
                .map_err(|_| Error::Clock)?
            } else {
                rescale(origin as i128, video.time_base, source.time_base)?
            };
            muxer.tracks[source.index] = Some(Track {
                output: unsafe { (*stream).index as usize },
                input_clock,
                shift,
                last_dts: None,
                started: false,
            });
        }
        let buffer = unsafe { av::av_malloc(32768).cast::<u8>() };
        if buffer.is_null() {
            return Err(Error::Allocation);
        }
        muxer.io = unsafe {
            av::avio_alloc_context(
                buffer,
                32768,
                1,
                (&mut *muxer.output as *mut Output).cast(),
                None,
                Some(write),
                None,
            )
        };
        if muxer.io.is_null() {
            unsafe {
                av::av_free(buffer.cast());
            }
            return Err(Error::Allocation);
        }
        unsafe {
            (*muxer.format).pb = muxer.io;
            (*muxer.format).flags |= av::AVFMT_FLAG_CUSTOM_IO;
            (*muxer.format).avoid_negative_ts = av::AVFMT_AVOID_NEG_TS_DISABLED;
            if muxer.presented {
                (*muxer.format).max_interleave_delta = 500_000;
            }
        }
        let mut options = Dictionary(ptr::null_mut());
        options.set(c"movflags", c"frag_keyframe+delay_moov+default_base_moof")?;
        options.set(c"use_editlist", c"1")?;
        native(
            unsafe {
                av::av_dict_set_int(&mut options.0, c"movie_timescale".as_ptr(), timescale, 0)
            },
            "movie timescale",
        )?;
        let code = unsafe { av::avformat_write_header(muxer.format, &mut options.0) };
        muxer.check_native(code, "write header")?;
        Ok(muxer)
    }
    fn check_native(&self, code: i32, operation: &'static str) -> Result<(), Error> {
        self.job.check()?;
        native(code, operation).map_err(|error| self.job.fail(error))?;
        if !self.io.is_null() {
            native(unsafe { (*self.io).error }, "output callback")
                .map_err(|error| self.job.fail(error))?;
        }
        Ok(())
    }
    pub fn push(&mut self, delivery: &Delivery) -> Result<(), Error> {
        let result = self.push_inner(delivery, None);
        result.map_err(|error| self.job.fail(error))
    }
    pub fn push_presented_video(
        &mut self,
        delivery: &Delivery,
        pts: i64,
        duration: i64,
    ) -> Result<(), Error> {
        if delivery.configuration()?[delivery.packet()?.stream_index()].codec != Codec::H264 {
            return Err(self.job.fail(Error::Configuration));
        }
        let result = self.push_inner(delivery, Some((pts, duration)));
        result.map_err(|error| self.job.fail(error))
    }
    fn push_inner(
        &mut self,
        delivery: &Delivery,
        presentation: Option<(i64, i64)>,
    ) -> Result<(), Error> {
        self.job.check()?;
        if self.presented != presentation.is_some() {
            return Err(Error::Configuration);
        }
        if self.finished {
            return Err(Error::Closed);
        }
        self.job.source.accepts(delivery)?;
        let packet = delivery.packet()?;
        if (self.sequence.is_none() && packet.sequence() != self.first_sequence)
            || self.sequence.is_some_and(|last| packet.sequence() <= last)
        {
            return Err(Error::Order);
        }
        self.sequence = Some(packet.sequence());
        let Some(track) = self
            .tracks
            .get_mut(packet.stream_index())
            .ok_or(Error::Configuration)?
            .as_mut()
        else {
            return Ok(());
        };
        let (pts, dts, duration) = match presentation {
            Some((pts, duration)) => (pts, pts, duration),
            None => (
                packet.pts().ok_or(Error::Clock)?,
                packet.dts().ok_or(Error::Clock)?,
                packet.duration(),
            ),
        };
        if track.last_dts.is_some_and(|last| dts <= last) || duration <= 0 {
            return Err(Error::Clock);
        }
        let total = self
            .input_bytes
            .checked_add(packet.owned_bytes())
            .ok_or(Error::Limit)?;
        if total > self.limits.input_bytes || self.packets >= self.limits.packets {
            return Err(Error::Limit);
        }
        let copy = MutablePacket(unsafe { av::av_packet_alloc() });
        if copy.0.is_null() {
            return Err(Error::Allocation);
        }
        native(
            unsafe { av::av_packet_ref(copy.0, packet.as_ptr()) },
            "reference packet",
        )?;
        let out_stream = unsafe { &**(*self.format).streams.add(track.output) };
        let output_clock = TimeBase {
            numerator: out_stream.time_base.num,
            denominator: out_stream.time_base.den,
        };
        unsafe {
            (*copy.0).pts = rescale(
                pts as i128 - track.shift as i128,
                track.input_clock,
                output_clock,
            )?;
            (*copy.0).dts = rescale(
                dts as i128 - track.shift as i128,
                track.input_clock,
                output_clock,
            )?;
            (*copy.0).duration = rescale(duration as i128, track.input_clock, output_clock)?;
            (*copy.0).stream_index = track.output as i32;
            (*copy.0).pos = -1;
            if delivery.configuration()?[packet.stream_index()].codec == Codec::H264 {
                let idr = nals(packet.data())?.iter().any(|nal| nal[0] & 31 == 5);
                if !track.started && !idr {
                    return Err(Error::Configuration);
                }
                (*copy.0).flags = ((*copy.0).flags & !av::AV_PKT_FLAG_KEY)
                    | if idr { av::AV_PKT_FLAG_KEY } else { 0 };
            }
        }
        track.last_dts = Some(dts);
        track.started = true;
        self.input_bytes = total;
        self.packets += 1;
        let code = unsafe { av::av_interleaved_write_frame(self.format, copy.0) };
        self.check_native(code, "mux packet")
    }
    pub fn guard(&self) -> WriteGuard {
        WriteGuard(self.job.clone())
    }
    pub fn push_aac(&mut self, packet: &AacPacket) -> Result<(), Error> {
        let result = self.push_aac_inner(packet);
        result.map_err(|error| self.job.fail(error))
    }
    fn push_aac_inner(&mut self, packet: &AacPacket) -> Result<(), Error> {
        self.job.check()?;
        if !self.presented
            || self.finished
            || self
                .audio_parameters
                .as_ref()
                .is_none_or(|p| !Arc::ptr_eq(p, packet.parameters()))
        {
            return Err(Error::Configuration);
        }
        self.job.source.accepts_guard(packet.guard())?;
        let (pts, duration, rate) = packet.position()?;
        let bytes = packet.data()?.len();
        let track = self
            .tracks
            .iter_mut()
            .flatten()
            .find(|track| track.output == 1)
            .ok_or(Error::Configuration)?;
        if track.input_clock.denominator != rate as i32
            || track.last_dts.is_some_and(|last| pts <= last)
        {
            return Err(Error::Clock);
        }
        let total = self.input_bytes.checked_add(bytes).ok_or(Error::Limit)?;
        if total > self.limits.input_bytes || self.packets >= self.limits.packets {
            return Err(Error::Limit);
        }
        let copy = MutablePacket(unsafe { av::av_packet_alloc() });
        if copy.0.is_null() {
            return Err(Error::Allocation);
        }
        native(
            unsafe { av::av_packet_ref(copy.0, packet.as_ptr()) },
            "reference recovered AAC",
        )?;
        let stream = unsafe { &**(*self.format).streams.add(track.output) };
        let output_clock = TimeBase {
            numerator: stream.time_base.num,
            denominator: stream.time_base.den,
        };
        unsafe {
            (*copy.0).pts = rescale(
                i128::from(pts) - i128::from(track.shift),
                track.input_clock,
                output_clock,
            )?;
            (*copy.0).dts = (*copy.0).pts;
            (*copy.0).duration = rescale(i128::from(duration), track.input_clock, output_clock)?;
            (*copy.0).stream_index = track.output as i32;
            (*copy.0).pos = -1;
        }
        track.last_dts = Some(pts);
        track.started = true;
        self.input_bytes = total;
        self.packets += 1;
        let code = unsafe { av::av_interleaved_write_frame(self.format, copy.0) };
        self.check_native(code, "mux recovered AAC")
    }
    /// Drains emitted bytes without flushing/finishing the media selection. A
    /// chunk may split MP4 boxes; downstream framing must not assume one fragment.
    pub fn drain(&mut self) -> Result<Chunk, Error> {
        self.job.check()?;
        if self.chunks >= self.limits.chunks {
            return Err(self.job.fail(Error::Limit));
        }
        unsafe {
            av::avio_flush(self.io);
        }
        self.check_native(0, "flush output")?;
        self.chunks += 1;
        Ok(Chunk {
            bytes: std::mem::take(&mut self.output.pending),
            job: self.job.clone(),
        })
    }
    /// Explicit clip-local completion. Drop, camera EOF and source failure never
    /// call this implicitly. The source subscription must remain valid through it.
    pub fn finish(&mut self) -> Result<Chunk, Error> {
        self.job.check()?;
        if self.finished {
            return Err(Error::Closed);
        }
        if self.tracks.iter().flatten().any(|track| !track.started) {
            return Err(self.job.fail(Error::Incomplete));
        }
        let code = unsafe { av::av_write_trailer(self.format) };
        self.check_native(code, "write trailer")?;
        let chunk = self.drain()?;
        self.finished = true;
        Ok(chunk)
    }
    /// Bytes accepted by this job's in-memory AVIO callback, not by HDS/Home.
    pub fn accepted_bytes(&self) -> usize {
        self.output.total
    }
}
