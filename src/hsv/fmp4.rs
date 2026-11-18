//! Shared-source recording history and clip-local Rust fMP4 jobs. No child,
//! alternate reader, or persistent mux whose init belongs to another clip.
#[cfg(test)]
#[path = "recording_camera_tests.rs"]
mod camera_tests;
#[cfg(test)]
#[path = "fmp4_tests.rs"]
mod tests;
use super::lease::Lease;
use crate::metrics::{ErrorSubsystem, Metrics};
use amcrust::{
    ingest::Cancellation,
    media::{
        audio, fmp4,
        recording::{self, Frame, Processor},
    },
    source::{Source, SubscriptionLimits, Tracks},
};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::{Mutex, broadcast, mpsc, watch};

const MAX_BOX: usize = 16 * 1024 * 1024;
const MAX_INIT: usize = 1024 * 1024;
const MAX_HISTORY: usize = 12 * 1024 * 1024;
const HISTORY_NS: i128 = 12_000_000_000;

#[derive(Clone, Debug)]
pub struct Fragment {
    pub data: Arc<Vec<u8>>,
    pub lease: Lease,
}
pub enum Message {
    Init(Fragment),
    Media(Fragment),
    End,
}
#[derive(Clone, Debug, PartialEq)]
pub struct RecorderConfig {
    pub width: u16,
    pub height: u16,
    pub fps: u8,
    pub audio: bool,
    pub audio_sample_rate_hz: u32,
    pub audio_bitrate: u32,
    pub fragment_ms: u32,
    pub prebuffer_ms: u32,
}
struct Entry {
    frame: Arc<Frame>,
    bytes: usize,
    end: i128,
}
#[derive(Default)]
struct RecorderState {
    frames: VecDeque<Entry>,
    bytes: usize,
    latest: i128,
    live: Option<broadcast::Sender<Arc<Frame>>>,
    epoch: Option<Lease>,
    owner: Option<Arc<ProducerOwner>>,
    stop: Option<Lease>,
    config: Option<RecorderConfig>,
    clips: Vec<Arc<ClipOwner>>,
}
impl RecorderState {
    fn clear(&mut self) {
        if let Some(epoch) = self.epoch.take() {
            epoch.revoke();
        }
        self.frames.clear();
        self.bytes = 0;
        self.latest = 0;
        self.live = None;
    }
    fn publish(&mut self, frame: Arc<Frame>) -> Result<(), String> {
        let bytes = frame.bytes().map_err(|e| e.to_string())?;
        let end = frame.end_ns().map_err(|e| e.to_string())?;
        self.latest = self.latest.max(end);
        while self
            .frames
            .front()
            .is_some_and(|entry| entry.end < self.latest - HISTORY_NS)
        {
            self.bytes -= self.frames.pop_front().unwrap().bytes;
        }
        if self.bytes + bytes > MAX_HISTORY || self.frames.len() >= 1536 {
            return Err("recording history budget".into());
        }
        self.bytes += bytes;
        self.frames.push_back(Entry {
            frame: frame.clone(),
            bytes,
            end,
        });
        // Snapshot and this publication share the state lock: no replay/live overlap.
        if let Some(live) = &self.live {
            let _ = live.send(frame);
        }
        Ok(())
    }
}
struct ProducerOwner(Mutex<Option<tokio::task::JoinHandle<()>>>);
impl ProducerOwner {
    async fn join(&self) {
        let mut owner = self.0.lock().await;
        if let Some(task) = owner.as_mut() {
            let _ = task.await;
        }
        owner.take();
    }
}
#[derive(Default)]
struct Lifetime(std::sync::Mutex<Option<Lease>>);
impl Drop for Lifetime {
    fn drop(&mut self) {
        if let Some(lease) = self.0.get_mut().unwrap_or_else(|p| p.into_inner()).take() {
            lease.revoke();
        }
    }
}
#[derive(Clone)]
pub struct Recorder {
    lifetime: Arc<Lifetime>,
    state: Arc<Mutex<RecorderState>>,
    operation: Arc<Mutex<()>>,
    source: Source,
    audio_pool: audio::Pool,
    mux_pool: fmp4::Pool,
    metrics: Arc<Metrics>,
}
#[derive(Debug, serde::Serialize)]
pub struct RecordingResources {
    pub aac: amcrust::media::PoolSnapshot,
    pub mux: amcrust::media::PoolSnapshot,
    pub history: Option<HistoryResources>,
}
#[derive(Debug, serde::Serialize)]
pub struct HistoryResources {
    pub pipeline_running: bool,
    pub bytes: usize,
    pub entries: usize,
    pub end_span_seconds: f64,
    pub clip_owners: usize,
}

impl Recorder {
    /// Do not wait behind publication/teardown on the metrics HTTP path.
    pub fn resources(&self) -> RecordingResources {
        RecordingResources {
            aac: self.audio_pool.snapshot(),
            mux: self.mux_pool.snapshot(),
            history: self.state.try_lock().ok().map(|state| HistoryResources {
                pipeline_running: state.stop.as_ref().is_some_and(Lease::current),
                bytes: state.bytes,
                entries: state.frames.len(),
                clip_owners: state.clips.len(),
                end_span_seconds: state.frames.front().map_or(0.0, |oldest| {
                    (state.latest - oldest.end).max(0) as f64 / 1_000_000_000.0
                }),
            }),
        }
    }
    pub fn new(source: Source, metrics: Arc<Metrics>) -> Self {
        Self {
            lifetime: Arc::new(Lifetime::default()),
            state: Arc::new(Mutex::new(RecorderState::default())),
            operation: Arc::new(Mutex::new(())),
            source,
            audio_pool: audio::Pool::new(1, audio::AAC_RESERVATION_BYTES)
                .expect("fixed recording audio pool"),
            mux_pool: fmp4::Pool::new(1, 140 * 1024 * 1024).expect("fixed recording mux pool"),
            metrics,
        }
    }
    pub async fn start(&self, config: RecorderConfig) {
        let _operation = self.operation.lock().await;
        {
            let state = self.state.lock().await;
            if state.stop.as_ref().is_some_and(Lease::current)
                && state.config.as_ref() == Some(&config)
            {
                return;
            }
        }
        self.stop_inner().await;
        if config.prebuffer_ms > 4000
            || config.fragment_ms == 0
            || config.fragment_ms > 4000
            || !(1..=60).contains(&config.fps)
            || config.width == 0
            || config.height == 0
            || (config.audio
                && (!matches!(config.audio_sample_rate_hz, 32000 | 48000)
                    || !(16000..=192000).contains(&config.audio_bitrate)))
        {
            self.metrics.error(ErrorSubsystem::Recording);
            return;
        }
        let stop = Lease::new();
        *self.lifetime.0.lock().unwrap_or_else(|p| p.into_inner()) = Some(stop.clone());
        let state = self.state.clone();
        let source = self.source.clone();
        let pool = self.audio_pool.clone();
        let metrics = self.metrics.clone();
        let owner_stop = stop.clone();
        let owner_config = config.clone();
        let mut installed = self.state.lock().await;
        let owner = tokio::spawn(async move {
            let mut retry = 1;
            while owner_stop.current() {
                let epoch = Lease::child(owner_stop.clone());
                {
                    let mut state = state.lock().await;
                    state.clear();
                    state.epoch = Some(epoch.clone());
                    state.live = Some(broadcast::channel(128).0);
                }
                let runtime = tokio::runtime::Handle::current();
                let worker_state = state.clone();
                let worker_source = source.clone();
                let worker_pool = pool.clone();
                let worker_config = owner_config.clone();
                let worker_epoch = epoch.clone();
                let result = tokio::task::spawn_blocking(move || {
                    runtime.block_on(produce(
                        worker_source,
                        worker_state,
                        worker_config,
                        worker_pool,
                        worker_epoch,
                    ))
                })
                .await;
                epoch.revoke();
                state.lock().await.clear();
                if !owner_stop.current() {
                    break;
                }
                metrics.error(ErrorSubsystem::Recording);
                log::warn!("shared recording epoch ended: {:?}", result);
                tokio::select! { _ = owner_stop.retired() => break, _ = tokio::time::sleep(Duration::from_secs(retry)) => {} }
                retry = (retry * 2).min(30);
            }
        });
        installed.config = Some(config);
        installed.stop = Some(stop);
        installed.owner = Some(Arc::new(ProducerOwner(Mutex::new(Some(owner)))));
    }
    pub async fn stop(&self) {
        let _operation = self.operation.lock().await;
        self.stop_inner().await;
    }
    async fn stop_inner(&self) {
        let (owner, clips) = {
            let mut state = self.state.lock().await;
            if let Some(stop) = state.stop.take() {
                stop.revoke();
            }
            state.clear();
            state.config = None;
            (state.owner.clone(), state.clips.clone())
        };
        if let Some(owner) = owner {
            owner.join().await;
        }
        for clip in clips {
            clip.stop().await;
        }
        {
            let mut state = self.state.lock().await;
            state.owner = None;
            state.clips.clear();
        }
        self.lifetime
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
    }
    #[cfg(test)]
    pub async fn is_running(&self) -> bool {
        self.state
            .lock()
            .await
            .stop
            .as_ref()
            .is_some_and(Lease::current)
    }
    pub async fn open(&self) -> Result<Clip, String> {
        let _operation = self.operation.lock().await;
        let old = {
            let state = self.state.lock().await;
            if state.clips.iter().any(|clip| {
                !clip.finished.load(std::sync::atomic::Ordering::Acquire)
                    || Arc::strong_count(clip) > 1
            }) {
                return Err("recording clip busy".into());
            }
            state.clips.clone()
        };
        for clip in old {
            clip.stop().await;
        }
        self.state.lock().await.clips.clear();
        if self.mux_pool.reserved_bytes() != 0 {
            return Err("recording output still charged".into());
        }
        let mut state = self.state.lock().await;
        let config = state.config.as_ref().ok_or("recording disabled")?;
        let epoch = state
            .epoch
            .as_ref()
            .filter(|epoch| epoch.current())
            .ok_or("recording unavailable")?
            .clone();
        let latest_frame = state
            .frames
            .iter()
            .rev()
            .find(|entry| matches!(&*entry.frame, Frame::Video { .. }))
            .ok_or("recording warming up")?;
        if let Frame::Video { delivery, .. } = &*latest_frame.frame {
            if delivery
                .packet()
                .map_err(|e| format!("{e:?}"))?
                .received_at()
                .elapsed()
                > Duration::from_secs(1)
            {
                return Err("recording history is stale".into());
            }
        }
        let latest_video = state
            .frames
            .iter()
            .rev()
            .find(|entry| matches!(&*entry.frame, Frame::Video { .. }))
            .ok_or("recording warming up")?
            .end;
        let target = latest_video - i128::from(config.prebuffer_ms) * 1_000_000;
        let first = state
            .frames
            .iter()
            .rev()
            .find(|entry| {
                entry.frame.is_idr() && entry.frame.start_ns().is_ok_and(|pts| pts <= target)
            })
            .ok_or("no covering IDR history")?
            .frame
            .clone();
        let start = first.start_ns().map_err(|e| e.to_string())?;
        let audio_start = if config.audio {
            let before = start - 2048 * 1_000_000_000 / i128::from(config.audio_sample_rate_hz);
            Some(
                state
                    .frames
                    .iter()
                    .rev()
                    .find(|entry| {
                        matches!(&*entry.frame, Frame::Audio(_))
                            && entry.frame.start_ns().is_ok_and(|pts| pts <= before)
                    })
                    .ok_or("no covering AAC preroll")?
                    .frame
                    .start_ns()
                    .map_err(|e| e.to_string())?,
            )
        } else {
            None
        };
        let frames: Vec<_> = state
            .frames
            .iter()
            .filter(|entry| {
                entry.frame.start_ns().is_ok_and(|pts| match &*entry.frame {
                    Frame::Video { .. } => pts >= start,
                    Frame::Audio(_) => audio_start.is_some_and(|begin| pts >= begin),
                })
            })
            .map(|entry| entry.frame.clone())
            .collect();
        let live = state
            .live
            .as_ref()
            .ok_or("recording unavailable")?
            .subscribe();
        let lease = Lease::child(epoch);
        let (tx, rx) = mpsc::channel(2);
        let (finish, finishing) = watch::channel(false);
        let worker_lease = lease.clone();
        let pool = self.mux_pool.clone();
        let metrics = self.metrics.clone();
        let runtime = tokio::runtime::Handle::current();
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completed = finished.clone();
        let owner = tokio::task::spawn_blocking(move || {
            struct Finished {
                done: Arc<std::sync::atomic::AtomicBool>,
                lease: Lease,
                completed: bool,
            }
            impl Drop for Finished {
                fn drop(&mut self) {
                    if !self.completed {
                        self.lease.revoke();
                    }
                    self.done.store(true, std::sync::atomic::Ordering::Release);
                }
            }
            let mut completed = Finished {
                done: completed,
                lease: worker_lease.clone(),
                completed: false,
            };
            let result = runtime.block_on(render(
                frames,
                live,
                first,
                pool,
                worker_lease.clone(),
                finishing,
                tx,
                metrics,
            ));
            completed.completed = result.is_ok();
            result
        });
        let owner = Arc::new(ClipOwner {
            lease: lease.clone(),
            task: Mutex::new(Some(owner)),
            finished,
        });
        state.clips.push(owner.clone());
        Ok(Clip {
            messages: rx,
            finish,
            lease,
            owner,
        })
    }
}
async fn produce(
    source: Source,
    state: Arc<Mutex<RecorderState>>,
    config: RecorderConfig,
    pool: audio::Pool,
    epoch: Lease,
) -> Result<(), String> {
    let mut subscription = source
        .subscribe(
            if config.audio {
                Tracks::AudioVideo
            } else {
                Tracks::Video
            },
            SubscriptionLimits {
                packets: 1024,
                bytes: MAX_HISTORY,
                residence: Duration::from_secs(20),
                media_span: Duration::from_secs(20),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut processor = None;
    let cancellation = Cancellation::default();
    let mut startup = None;
    let mut last_video = None;
    let mut last_audio = None;
    let mut video_ready = false;
    let result = async {
        loop {
            let delivery = tokio::select! { biased; _ = epoch.retired() => return Ok(()), result = tokio::time::timeout(Duration::from_secs(20), subscription.recv()) => result.map_err(|_| "recording source deadline")?.map_err(|e| format!("{e:?}"))? };
            let packet = delivery.packet().map_err(|e| format!("{e:?}"))?;
            let at = packet.received_at();
            if at.elapsed() > Duration::from_secs(1) { return Err("recording input is stale".into()); }
            match delivery.configuration().map_err(|e| format!("{e:?}"))?[packet.stream_index()].codec {
                amcrust::ingest::Codec::H264 => last_video = Some(at),
                amcrust::ingest::Codec::Aac => last_audio = Some(at),
            }
            if processor.is_none() {
                startup = Some(tokio::time::Instant::now());
                processor = Some(Processor::new(&delivery, recording::Config { width: config.width, height: config.height, fps: config.fps, audio_rate: config.audio.then_some(config.audio_sample_rate_hz), audio_bitrate: config.audio_bitrate }, pool.clone(), cancellation.clone()).map_err(|e| e.to_string())?);
            }
            let processor = processor.as_mut().unwrap();
            let frames = processor.push(delivery).map_err(|e| format!("{e}; {:?}", processor.status()))?;
            let mut state = state.lock().await;
            if !epoch.current() { return Ok(()); }
            for frame in frames { video_ready |= frame.is_idr(); state.publish(frame)?; }
            if video_ready && last_video.is_some_and(|at| at.elapsed() > Duration::from_secs(1)) { return Err("recording video gap".into()); }
            if config.audio && (last_audio.is_some_and(|at| at.elapsed() > Duration::from_secs(1)) || (last_audio.is_none() && startup.unwrap().elapsed() > Duration::from_secs(3))) { return Err("recording audio gap".into()); }
            if !video_ready && startup.unwrap().elapsed() > Duration::from_secs(15) { return Err("recording video admission deadline".into()); }
        }
    }.await;
    epoch.revoke();
    cancellation.cancel();
    result
}

struct ClipOwner {
    lease: Lease,
    task: Mutex<Option<tokio::task::JoinHandle<Result<(), String>>>>,
    finished: Arc<std::sync::atomic::AtomicBool>,
}
impl ClipOwner {
    async fn stop(&self) {
        self.lease.revoke();
        let mut task = self.task.lock().await;
        if let Some(owner) = task.as_mut() {
            let _ = owner.await;
        }
        task.take();
    }
}
pub struct Clip {
    pub messages: mpsc::Receiver<Message>,
    pub lease: Lease,
    finish: watch::Sender<bool>,
    owner: Arc<ClipOwner>,
}
impl Clip {
    pub fn finish(&self) {
        self.finish.send_replace(true);
    }
    pub async fn stop(&mut self) {
        self.lease.revoke();
        self.messages.close();
        self.owner.stop().await;
        while self.messages.try_recv().is_ok() {}
    }
}
impl Drop for Clip {
    fn drop(&mut self) {
        self.lease.revoke();
    }
}

struct Boxes {
    pending: Vec<u8>,
    init: Vec<u8>,
    fragment: Vec<u8>,
    initialized: bool,
}
impl Boxes {
    fn new() -> Self {
        Self {
            pending: Vec::new(),
            init: Vec::new(),
            fragment: Vec::new(),
            initialized: false,
        }
    }
    fn feed(&mut self, bytes: &[u8], lease: &Lease) -> Result<Vec<Message>, String> {
        if self.pending.len() + bytes.len() > MAX_BOX * 2 {
            return Err("MP4 parser budget".into());
        }
        self.pending.extend_from_slice(bytes);
        let mut offset = 0;
        let mut messages = Vec::new();
        while self.pending.len() - offset >= 8 {
            let data = &self.pending[offset..];
            let mut size = u64::from(u32::from_be_bytes(data[..4].try_into().unwrap()));
            let header = if size == 1 {
                if data.len() < 16 {
                    break;
                }
                size = u64::from_be_bytes(data[8..16].try_into().unwrap());
                16
            } else {
                8
            };
            if size < header || size > MAX_BOX as u64 {
                return Err("invalid MP4 box size".into());
            }
            let size = size as usize;
            if data.len() < size {
                break;
            }
            let kind = &data[4..8];
            if kind == b"moof" {
                if !self.fragment.is_empty() {
                    return Err("unpaired MP4 moof".into());
                }
                if !self.initialized {
                    if self.init.is_empty() {
                        return Err("missing MP4 init".into());
                    }
                    self.initialized = true;
                    messages.push(Message::Init(Fragment {
                        data: Arc::new(std::mem::take(&mut self.init)),
                        lease: lease.clone(),
                    }));
                }
                self.fragment.extend_from_slice(&data[..size]);
            } else if kind == b"mdat" {
                if self.fragment.is_empty() || self.fragment.len() + size > MAX_BOX {
                    return Err("invalid MP4 media pair".into());
                }
                self.fragment.extend_from_slice(&data[..size]);
                messages.push(Message::Media(Fragment {
                    data: Arc::new(std::mem::take(&mut self.fragment)),
                    lease: lease.clone(),
                }));
            } else if !self.initialized {
                if self.init.len() + size > MAX_INIT {
                    return Err("MP4 init budget".into());
                }
                self.init.extend_from_slice(&data[..size]);
            } else if kind != b"mfra" {
                return Err("unexpected MP4 box after init".into());
            }
            offset += size;
        }
        self.pending.drain(..offset);
        Ok(messages)
    }
    fn complete(&self) -> bool {
        self.initialized && self.pending.is_empty() && self.fragment.is_empty()
    }
}
async fn emitted(
    mux: &mut fmp4::Muxer,
    boxes: &mut Boxes,
    lease: &Lease,
    tx: &mpsc::Sender<Message>,
    metrics: &Metrics,
    finish: bool,
) -> Result<(), String> {
    let chunk = if finish { mux.finish() } else { mux.drain() }.map_err(|e| e.to_string())?;
    for message in boxes.feed(chunk.bytes().map_err(|e| e.to_string())?, lease)? {
        if let Message::Media(fragment) = &message {
            metrics.recording_fragment(fragment.data.len());
        }
        tokio::select! { biased; _ = lease.retired() => return Err("recording retired".into()), result = tokio::time::timeout(Duration::from_secs(2), tx.send(message)) => result.map_err(|_| "recording output pressure")?.map_err(|_| "recording receiver closed")? }
    }
    Ok(())
}
async fn render(
    frames: Vec<Arc<Frame>>,
    mut live: broadcast::Receiver<Arc<Frame>>,
    first: Arc<Frame>,
    pool: fmp4::Pool,
    lease: Lease,
    mut finishing: watch::Receiver<bool>,
    tx: mpsc::Sender<Message>,
    metrics: Arc<Metrics>,
) -> Result<(), String> {
    let Frame::Video { delivery, pts, .. } = &*first else {
        return Err("clip does not start at video".into());
    };
    let audio = frames.iter().find_map(|frame| {
        if let Frame::Audio(packet) = &**frame {
            Some(packet)
        } else {
            None
        }
    });
    let has_audio = audio.is_some();
    let audio_unit_ns = audio
        .map(|packet| {
            packet.position().map(|(_, _, rate)| {
                (1024i128 * 1_000_000_000 + i128::from(rate) - 1) / i128::from(rate)
            })
        })
        .transpose()
        .map_err(|e| e.to_string())?
        .unwrap_or(0);
    let cancellation = Cancellation::default();
    let mut mux = fmp4::Muxer::new_presented(
        delivery,
        *pts,
        audio,
        &pool,
        fmp4::Limits {
            input_bytes: 64 * 1024 * 1024,
            output_bytes: 64 * 1024 * 1024,
            packets: 16384,
            chunks: 16384,
            lifetime: Duration::from_secs(120),
        },
        cancellation.clone(),
    )
    .map_err(|e| e.to_string())?;
    drop(first);
    lease.bind(mux.guard())?;
    let mut boxes = Boxes::new();
    let mut snapshot = frames.into_iter();
    let mut video_end = 0;
    let mut audio_end = 0;
    let mut cut = None;
    let mut input_bytes = 0usize;
    loop {
        if (*finishing.borrow() || input_bytes >= 48 * 1024 * 1024) && cut.is_none() {
            // Whole-AU tail: never finish an undercovered video selection. If AAC
            // is already too far ahead, refuse rather than invent a video hold.
            if has_audio && audio_end > video_end + audio_unit_ns {
                return Err("recording tail is not covered".into());
            }
            cut = Some((video_end, tokio::time::Instant::now()));
        }
        if let Some((end, at)) = cut {
            if !has_audio || audio_end >= end {
                break;
            }
            if at.elapsed() > Duration::from_secs(2) {
                return Err("covering AAC tail deadline".into());
            }
        }
        let frame = if let Some(frame) = snapshot.next() {
            frame
        } else {
            tokio::select! { biased;
                _ = lease.retired() => return Err("clip retired".into()),
                _ = finishing.changed(), if cut.is_none() => continue,
                result = tokio::time::timeout(Duration::from_secs(2), live.recv()) => result.map_err(|_| "clip input deadline")?.map_err(|_| "clip input lost")?,
            }
        };
        input_bytes = input_bytes
            .checked_add(frame.bytes().map_err(|e| e.to_string())?)
            .ok_or("clip byte counter overflow")?;
        match &*frame {
            Frame::Video {
                delivery,
                pts,
                duration,
                ..
            } => {
                if cut.is_some() {
                    continue;
                }
                mux.push_presented_video(delivery, *pts, *duration)
                    .map_err(|e| e.to_string())?;
                video_end = frame.end_ns().map_err(|e| e.to_string())?;
            }
            Frame::Audio(packet) => {
                mux.push_aac(packet).map_err(|e| e.to_string())?;
                audio_end = frame.end_ns().map_err(|e| e.to_string())?;
            }
        }
        emitted(&mut mux, &mut boxes, &lease, &tx, &metrics, false).await?;
    }
    emitted(&mut mux, &mut boxes, &lease, &tx, &metrics, true).await?;
    if !boxes.complete() {
        return Err("incomplete MP4 finalization".into());
    }
    tokio::select! { _ = lease.retired() => Err("clip retired".into()), result = tx.send(Message::End) => result.map_err(|_| "clip receiver closed".into()) }
}

/// Read-only exercise of the production source/history/mux path. No capture is retained.
pub async fn probe_recording_pipeline(
    source: Source,
    config: &RecorderConfig,
    seconds: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !(1..=90).contains(&seconds) {
        return Err("recording probe duration must be 1..90 seconds".into());
    }
    let recorder = Recorder::new(source, Metrics::new());
    recorder.start(config.clone()).await;
    let startup_deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    let mut clip = loop {
        match recorder.open().await {
            Ok(clip) => break clip,
            Err(_) if tokio::time::Instant::now() < startup_deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await
            }
            Err(error) => {
                recorder.stop().await;
                return Err(error.into());
            }
        }
    };
    let finish_at = tokio::time::Instant::now() + Duration::from_secs(u64::from(seconds));
    let deadline = finish_at + Duration::from_secs(5);
    let mut finishing = false;
    let result = loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break Err("recording probe deadline"),
            _ = tokio::time::sleep_until(finish_at), if !finishing => { finishing = true; clip.finish(); },
            message = clip.messages.recv() => match message {
                Some(Message::End) => break Ok(()), Some(_) => {}, None => break Err("recording probe incomplete"),
            },
        }
    };
    clip.stop().await;
    recorder.stop().await;
    result.map_err(Into::into)
}
