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
use super::{Error, rescale};
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
        })
    }
    pub fn reserved_bytes(&self) -> usize {
        self.capacity - self.bytes.available_permits()
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
impl Chunk {
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
        let packet = first.packet()?;
        let streams = first.configuration()?;
        let video = &streams[packet.stream_index()];
        if video.codec != Codec::H264 || !nals(packet.data())?.iter().any(|nal| nal[0] & 31 == 5) {
            return Err(Error::Configuration);
        }
        if audio && !streams.iter().any(|s| s.codec == Codec::Aac) {
            return Err(Error::Configuration);
        }
        let origin = packet.pts().ok_or(Error::Clock)?;
        let job = Arc::new(Job {
            source: first.guard()?,
            cancellation,
            deadline: Instant::now() + limits.lifetime,
            failure: Mutex::new(None),
            _permits: permits,
        });
        job.check()?;
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
            native(
                unsafe {
                    av::avcodec_parameters_copy((*stream).codecpar, source.parameters.as_ptr())
                },
                "copy codec parameters",
            )?;
            unsafe {
                (*(*stream).codecpar).codec_tag = 0;
                (*stream).time_base = av::AVRational {
                    num: source.time_base.numerator,
                    den: source.time_base.denominator,
                };
            }
            let denominator = source.time_base.denominator as i64;
            let gcd = unsafe { av::av_gcd(timescale, denominator) };
            timescale = timescale
                .checked_mul(denominator / gcd)
                .filter(|v| *v <= i32::MAX as i64)
                .ok_or(Error::Clock)?;
            let shift = rescale(origin as i128, video.time_base, source.time_base)?;
            muxer.tracks[source.index] = Some(Track {
                output: unsafe { (*stream).index as usize },
                input_clock: source.time_base,
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
        let result = self.push_inner(delivery);
        result.map_err(|error| self.job.fail(error))
    }
    fn push_inner(&mut self, delivery: &Delivery) -> Result<(), Error> {
        self.job.check()?;
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
        let (pts, dts) = (
            packet.pts().ok_or(Error::Clock)?,
            packet.dts().ok_or(Error::Clock)?,
        );
        if track.last_dts.is_some_and(|last| dts <= last) || packet.duration() <= 0 {
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
            (*copy.0).duration =
                rescale(packet.duration() as i128, track.input_clock, output_clock)?;
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
