//! Rust-owned, blocking libavformat acquisition. No worker, IPC or consumer transport.
//!
//! Construct and use `Reader` on a dedicated blocking thread. It is deliberately
//! !Send/!Sync; only immutable packet/configuration owners and `Cancellation` cross
//! threads. Cancellation is sticky and must precede joining the owning thread.
//! Open, discovery and each read have deadlines. Drop closes the input while its
//! interrupt state is still alive. No subscriptions are exposed by this layer.
//!
//! Source timestamps are never repaired or rebased here. Missing clocks stay None,
//! and all public packet side data (including RTCP SR) survives packet ownership.
use ffmpeg_sys_next as av;
use std::{
    ffi::CString,
    fmt,
    marker::PhantomData,
    ptr::{self, NonNull},
    rc::Rc,
    sync::{
        Arc, Once,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Deliberately contains no URL, native log text, or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidConfiguration,
    Allocation,
    Cancelled,
    Deadline,
    Native { operation: &'static str, code: i32 },
    UnsupportedLayout,
    PacketLimit,
    ConfigurationChanged,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "media acquisition: {self:?}")
    }
}
impl std::error::Error for Error {}

/// Acquisition-only secret. Intentionally no Debug/Display implementation.
#[derive(Clone)]
pub struct CameraUrl(CString);
impl CameraUrl {
    pub fn new(url: String) -> Result<Self, Error> {
        if !url.starts_with("rtsp://") {
            return Err(Error::InvalidConfiguration);
        }
        CString::new(url)
            .map(Self)
            .map_err(|_| Error::InvalidConfiguration)
    }
}

#[derive(Clone, Default)]
pub struct Cancellation(Arc<AtomicBool>);
impl Cancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
struct Interrupt {
    cancellation: Cancellation,
    origin: Instant,
    deadline_ms: AtomicU64,
}
impl Interrupt {
    fn elapsed_ms(&self) -> u64 {
        self.origin.elapsed().as_millis().min(u64::MAX as u128) as u64
    }
    fn arm(&self, duration: Duration) {
        self.deadline_ms.store(
            self.elapsed_ms()
                .saturating_add(duration.as_millis().min(u64::MAX as u128) as u64),
            Ordering::Release,
        );
    }
    fn reason(&self) -> Option<Error> {
        if self.cancellation.is_cancelled() {
            Some(Error::Cancelled)
        } else if self.elapsed_ms() >= self.deadline_ms.load(Ordering::Acquire) {
            Some(Error::Deadline)
        } else {
            None
        }
    }
}
// SAFETY: opaque always points to Reader's boxed Interrupt, installed before open
// and retained through avformat_close_input. No locks, allocation or panic paths.
unsafe extern "C" fn interrupt(opaque: *mut std::ffi::c_void) -> i32 {
    let state = unsafe { &*opaque.cast::<Interrupt>() };
    i32::from(state.reason().is_some())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeBase {
    pub numerator: i32,
    pub denominator: i32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    H264,
    Aac,
}

/// Deep-copy of public AVCodecParameters, including extradata, channel layout and
/// coded side data. No pointer back into the demuxer and no public mutable access.
pub struct CodecParameters(NonNull<av::AVCodecParameters>);
// SAFETY: independently allocated, immutable after construction, freed only after
// the final Arc is dropped. FFmpeg parameter destruction has no thread affinity.
unsafe impl Send for CodecParameters {}
unsafe impl Sync for CodecParameters {}
impl Drop for CodecParameters {
    fn drop(&mut self) {
        unsafe {
            let mut p = self.0.as_ptr();
            av::avcodec_parameters_free(&mut p);
        }
    }
}
impl CodecParameters {
    // Only audited Rust FFI consumers may borrow this; never hand it to an API
    // that mutates/frees the source parameters. Use avcodec_parameters_copy.
    pub(crate) fn as_ptr(&self) -> *const av::AVCodecParameters {
        self.0.as_ptr()
    }
    pub fn dimensions(&self) -> (i32, i32) {
        unsafe { ((*self.0.as_ptr()).width, (*self.0.as_ptr()).height) }
    }
    pub fn audio_format(&self) -> (i32, i32) {
        unsafe {
            (
                (*self.0.as_ptr()).sample_rate,
                (*self.0.as_ptr()).ch_layout.nb_channels,
            )
        }
    }
    /// Conservative charge for the public parameter allocation and owned buffers.
    pub fn owned_bytes(&self) -> usize {
        let p = unsafe { self.0.as_ref() };
        let side = if p.nb_coded_side_data == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(p.coded_side_data, p.nb_coded_side_data as usize) }
        };
        std::mem::size_of::<av::AVCodecParameters>()
            + self.extradata().len()
            + side
                .iter()
                .map(|s| s.size + std::mem::size_of::<av::AVPacketSideData>())
                .sum::<usize>()
            + p.ch_layout.nb_channels.max(0) as usize * std::mem::size_of::<av::AVChannelCustom>()
    }
    fn matches(&self, other: &av::AVCodecParameters) -> bool {
        let p = unsafe { self.0.as_ref() };
        p.codec_id == other.codec_id
            && p.codec_type == other.codec_type
            && p.format == other.format
            && p.profile == other.profile
            && p.level == other.level
            && p.width == other.width
            && p.height == other.height
            && p.sample_rate == other.sample_rate
            && p.frame_size == other.frame_size
            && p.video_delay == other.video_delay
            && unsafe { av::av_channel_layout_compare(&p.ch_layout, &other.ch_layout) == 0 }
            && self.extradata()
                == unsafe { bytes(other.extradata, other.extradata_size.max(0) as usize) }
    }
    pub fn extradata(&self) -> &[u8] {
        let p = unsafe { self.0.as_ref() };
        unsafe { bytes(p.extradata, p.extradata_size.max(0) as usize) }
    }
}

#[derive(Clone)]
pub struct StreamConfiguration {
    pub index: usize,
    pub codec: Codec,
    pub time_base: TimeBase,
    pub parameters: Arc<CodecParameters>,
}

/// Immutable, reference-counted FFmpeg packet buffer and packet-owned side data.
/// Consumers share Arc<Packet>; later mux/codec code must av_packet_ref into its
/// own mutable packet before rescaling or changing any field.
pub struct Packet {
    raw: NonNull<av::AVPacket>,
    generation: u64,
    sequence: u64,
    received_at: Instant,
}
// SAFETY: av_read_frame creates a private packet; it is never mutated after
// publication. Its buffer references are FFmpeg-refcounted and side data is owned.
unsafe impl Send for Packet {}
unsafe impl Sync for Packet {}
impl Drop for Packet {
    fn drop(&mut self) {
        unsafe {
            let mut p = self.raw.as_ptr();
            av::av_packet_free(&mut p);
        }
    }
}
fn timestamp(value: i64) -> Option<i64> {
    (value != av::AV_NOPTS_VALUE).then_some(value)
}
// Only called for FFmpeg-owned pointer/size pairs while their owner is borrowed.
unsafe fn bytes<'a>(data: *const u8, size: usize) -> &'a [u8] {
    if size == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(data, size) }
    }
}
impl Packet {
    // Consumers av_packet_ref into their own mutable packet before muxing.
    pub(crate) fn as_ptr(&self) -> *const av::AVPacket {
        self.raw.as_ptr()
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn received_at(&self) -> Instant {
        self.received_at
    }
    fn raw(&self) -> &av::AVPacket {
        unsafe { self.raw.as_ref() }
    }
    pub fn stream_index(&self) -> usize {
        self.raw().stream_index as usize
    }
    pub fn pts(&self) -> Option<i64> {
        timestamp(self.raw().pts)
    }
    pub fn dts(&self) -> Option<i64> {
        timestamp(self.raw().dts)
    }
    pub fn duration(&self) -> i64 {
        self.raw().duration
    }
    pub fn flags(&self) -> i32 {
        self.raw().flags
    }
    pub fn data(&self) -> &[u8] {
        unsafe { bytes(self.raw().data, self.raw().size as usize) }
    }
    pub fn side_data(&self) -> impl Iterator<Item = (av::AVPacketSideDataType, &[u8])> {
        let p = self.raw();
        let side = if p.side_data_elems == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(p.side_data, p.side_data_elems as usize) }
        };
        side.iter()
            .map(|s| (s.type_, unsafe { bytes(s.data, s.size) }))
    }
    /// Public FFmpeg 9 metadata, not private RTSP demuxer internals.
    pub fn sender_report(&self) -> Option<SenderReport> {
        self.side_data().find_map(|(kind, data)| {
            if kind != av::AVPacketSideDataType::AV_PKT_DATA_RTCP_SR
                || data.len() != std::mem::size_of::<av::AVRTCPSenderReport>()
            {
                return None;
            }
            // Side-data alignment is not assumed; copy the public POD value.
            let sr = unsafe { ptr::read_unaligned(data.as_ptr().cast::<av::AVRTCPSenderReport>()) };
            Some(SenderReport {
                ssrc: sr.ssrc,
                ntp: sr.ntp_timestamp,
                rtp: sr.rtp_timestamp,
            })
        })
    }
    pub fn owned_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + std::mem::size_of::<av::AVPacket>()
            + self.data().len()
            + self
                .side_data()
                .map(|(_, b)| b.len() + std::mem::size_of::<av::AVPacketSideData>())
                .sum::<usize>()
    }
}
#[derive(Debug, Clone, Copy)]
pub struct SenderReport {
    pub ssrc: u32,
    pub ntp: u64,
    pub rtp: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub startup: Duration,
    pub read: Duration,
    pub packet_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            startup: Duration::from_secs(10),
            read: Duration::from_secs(5),
            packet_bytes: 4 * 1024 * 1024,
        }
    }
}

pub struct Reader {
    input: *mut av::AVFormatContext,
    interrupt: Box<Interrupt>,
    streams: Vec<StreamConfiguration>,
    limits: Limits,
    generation: u64,
    sequence: u64,
    terminal: Option<Error>,
    _thread_bound: PhantomData<Rc<()>>,
}
impl Drop for Reader {
    fn drop(&mut self) {
        // Interrupt RTSP teardown too; do not leave close waiting on a dead peer.
        self.interrupt.cancellation.cancel();
        unsafe {
            av::avformat_close_input(&mut self.input);
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
        let code = unsafe { av::av_dict_set(&mut self.0, key.as_ptr(), value.as_ptr(), 0) };
        if code < 0 {
            Err(Error::Native {
                operation: "options",
                code,
            })
        } else {
            Ok(())
        }
    }
}
impl Reader {
    pub fn open(
        url: CameraUrl,
        generation: u64,
        cancellation: Cancellation,
        limits: Limits,
    ) -> Result<Self, Error> {
        if limits.startup.is_zero() || limits.read.is_zero() || limits.packet_bytes == 0 {
            return Err(Error::InvalidConfiguration);
        }
        static INIT: Once = Once::new();
        INIT.call_once(|| unsafe {
            // Native diagnostics can embed the entire credential-bearing URL.
            // Suppress them process-wide before any library I/O; report only typed
            // operation/code errors. Do not install a variadic Rust log callback.
            av::av_log_set_level(av::AV_LOG_QUIET);
        });
        let state = Box::new(Interrupt {
            cancellation,
            origin: Instant::now(),
            deadline_ms: AtomicU64::new(0),
        });
        state.arm(limits.startup);
        let input = unsafe { av::avformat_alloc_context() };
        if input.is_null() {
            return Err(Error::Allocation);
        }
        let mut reader = Self {
            input,
            interrupt: state,
            streams: Vec::new(),
            limits,
            generation,
            sequence: 0,
            terminal: None,
            _thread_bound: PhantomData,
        };
        unsafe {
            (*reader.input).interrupt_callback = av::AVIOInterruptCB {
                callback: Some(interrupt),
                opaque: (&mut *reader.interrupt as *mut Interrupt).cast(),
            };
        }
        reader.check(0, "open")?;
        let mut options = Dictionary(ptr::null_mut());
        options.set(c"rtsp_transport", c"tcp")?;
        options.set(c"protocol_whitelist", c"tcp,rtp")?;
        options.set(c"timeout", c"2000000")?;
        options.set(c"analyzeduration", c"200000")?;
        options.set(c"probesize", c"65536")?;
        options.set(c"fpsprobesize", c"2")?;
        let format = unsafe { av::av_find_input_format(c"rtsp".as_ptr()) };
        if format.is_null() {
            return Err(Error::UnsupportedLayout);
        }
        let code = unsafe {
            av::avformat_open_input(&mut reader.input, url.0.as_ptr(), format, &mut options.0)
        };
        reader.check(code, "open")?;
        let code = unsafe { av::avformat_find_stream_info(reader.input, ptr::null_mut()) };
        reader.check(code, "stream discovery")?;
        let input = unsafe { &*reader.input };
        if input.nb_streams == 0 || input.nb_streams > 2 {
            return Err(Error::UnsupportedLayout);
        }
        for index in 0..input.nb_streams as usize {
            let stream = unsafe { &**input.streams.add(index) };
            let parameters = unsafe { &*stream.codecpar };
            let codec = match parameters.codec_id {
                av::AVCodecID::AV_CODEC_ID_H264 => Codec::H264,
                av::AVCodecID::AV_CODEC_ID_AAC => Codec::Aac,
                _ => return Err(Error::UnsupportedLayout),
            };
            if reader.streams.iter().any(|s| s.codec == codec)
                || stream.time_base.num <= 0
                || stream.time_base.den <= 0
            {
                return Err(Error::UnsupportedLayout);
            }
            let copy = CodecParameters(
                NonNull::new(unsafe { av::avcodec_parameters_alloc() }).ok_or(Error::Allocation)?,
            );
            let code = unsafe { av::avcodec_parameters_copy(copy.0.as_ptr(), stream.codecpar) };
            reader.check(code, "codec parameters")?;
            reader.streams.push(StreamConfiguration {
                index,
                codec,
                time_base: TimeBase {
                    numerator: stream.time_base.num,
                    denominator: stream.time_base.den,
                },
                parameters: Arc::new(copy),
            });
        }
        Ok(reader)
    }
    fn check(&self, code: i32, operation: &'static str) -> Result<(), Error> {
        if let Some(reason) = self.interrupt.reason() {
            return Err(reason);
        }
        if code < 0 {
            Err(Error::Native { operation, code })
        } else {
            Ok(())
        }
    }
    pub fn streams(&self) -> &[StreamConfiguration] {
        &self.streams
    }
    /// Live EOF is an error, never a successful recording end. A caller must retire
    /// this generation on any error; this layer does not retry or guess clocks.
    pub fn read(&mut self) -> Result<Arc<Packet>, Error> {
        if let Some(error) = self.terminal {
            return Err(error);
        }
        let result = self.read_packet();
        if let Err(error) = &result {
            self.terminal = Some(*error);
        }
        result
    }
    fn read_packet(&mut self) -> Result<Arc<Packet>, Error> {
        self.interrupt.arm(self.limits.read);
        self.check(0, "read")?;
        let mut packet = Packet {
            raw: NonNull::new(unsafe { av::av_packet_alloc() }).ok_or(Error::Allocation)?,
            generation: self.generation,
            sequence: self.sequence,
            received_at: Instant::now(),
        };
        let code = unsafe { av::av_read_frame(self.input, packet.raw.as_ptr()) };
        self.check(code, "read")?;
        if packet.raw().size < 0
            || packet.raw().stream_index < 0
            || packet.stream_index() >= self.streams.len()
        {
            return Err(Error::UnsupportedLayout);
        }
        // Public stream descriptors and explicit packet change notifications are
        // generation boundaries, never silently applied to admitted consumers.
        let input = unsafe { &*self.input };
        if input.nb_streams as usize != self.streams.len() {
            return Err(Error::ConfigurationChanged);
        }
        for saved in &self.streams {
            let current = unsafe { &**input.streams.add(saved.index) };
            if current.time_base.num != saved.time_base.numerator
                || current.time_base.den != saved.time_base.denominator
                || !saved.parameters.matches(unsafe { &*current.codecpar })
            {
                return Err(Error::ConfigurationChanged);
            }
        }
        if packet.side_data().any(|(kind, _)| {
            matches!(
                kind,
                av::AVPacketSideDataType::AV_PKT_DATA_NEW_EXTRADATA
                    | av::AVPacketSideDataType::AV_PKT_DATA_PARAM_CHANGE
            )
        }) {
            return Err(Error::ConfigurationChanged);
        }
        if packet.owned_bytes() > self.limits.packet_bytes {
            return Err(Error::PacketLimit);
        }
        self.sequence = self.sequence.checked_add(1).ok_or(Error::PacketLimit)?;
        packet.received_at = Instant::now();
        Ok(Arc::new(packet))
    }
}
