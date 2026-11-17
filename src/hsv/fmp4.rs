//! Fragmented-MP4 recording pipeline: a persistent ffmpeg process stream-copies
//! the camera's RTSP feed into fMP4 (init segment + keyframe-aligned
//! fragments), which we parse into segments and keep in a rolling prebuffer.

use super::lease::Lease;
use log::{debug, error, warn};
use std::collections::VecDeque;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

const MAX_BOX: usize = 16 * 1024 * 1024;
const MAX_INIT: usize = 1024 * 1024;
const MAX_FRAGMENT: usize = 16 * 1024 * 1024;
const MAX_PREBUFFER: usize = 32 * 1024 * 1024;
use tokio::sync::{Mutex, broadcast};
use tokio::time::Instant;

use crate::metrics::{ErrorSubsystem, Metrics};

#[derive(Clone, Debug)]
pub struct Fragment {
    pub data: Arc<Vec<u8>>,
    pub at: Instant,
    pub lease: Lease,
}

#[derive(Default)]
struct RecorderState {
    init_segment: Option<Fragment>,
    prebuffer: VecDeque<Fragment>,
    owner: Option<tokio::task::JoinHandle<()>>,
    lease: Option<Lease>,
    generation: u64,
    config: Option<RecorderConfig>,
}

/// Encoder settings for the recording pipeline (already applied to the camera;
/// ffmpeg only stream-copies).
#[derive(Clone, Debug, PartialEq)]
pub struct RecorderConfig {
    pub rtsp_url: String,
    pub audio: bool,
    pub fps: u8,
    pub audio_sample_rate_hz: u32,
    pub fragment_ms: u32,
    pub prebuffer_ms: u32,
}

fn video_timestamp_bitstream_filter(fps: u8) -> String {
    // Build a monotonic decode clock from packet order. Preserve a valid
    // PTS-DTS composition offset so B-frames still display in presentation
    // order. `setts` edits encoded-packet metadata without transcoding.
    let fps = fps.max(1);
    format!(
        "setts=pts=N/({fps}*TB)+if(eq(PTS\\,NOPTS)\\,0\\,if(eq(DTS\\,NOPTS)\\,0\\,PTS-DTS)):dts=N/({fps}*TB):duration=1/({fps}*TB)"
    )
}

fn audio_timestamp_bitstream_filter(sample_rate_hz: u32) -> String {
    // AAC-LC packets contain 1024 samples, so packet order supplies an exact
    // monotonic clock even when camera timestamps are absent or duplicated.
    let sample_rate_hz = sample_rate_hz.max(1);
    format!("setts=ts=N*1024/({sample_rate_hz}*TB):duration=1024/({sample_rate_hz}*TB)")
}

fn recording_ffmpeg_command(config: &RecorderConfig) -> Command {
    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-hide_banner")
        .args(["-loglevel", "warning"])
        .args(["-fflags", "+genpts"])
        .args(["-rtsp_transport", "tcp"])
        .args(["-i", &config.rtsp_url])
        // Keep the MP4 track layout deterministic even if a camera lists
        // audio first in its RTSP session description.
        .args(["-map", "0:v:0"]);
    if config.audio {
        cmd.args(["-map", "0:a:0?"])
            .args(["-c:v", "copy"])
            .args(["-c:a", "copy"])
            .args([
                "-bsf:a",
                &audio_timestamp_bitstream_filter(config.audio_sample_rate_hz),
            ]);
    } else {
        cmd.arg("-an").args(["-c:v", "copy"]);
    }
    cmd.args(["-bsf:v", &video_timestamp_bitstream_filter(config.fps)])
        .args(["-f", "mp4"])
        .args(["-movflags", "frag_keyframe+empty_moov+default_base_moof"])
        .args(["-avoid_negative_ts", "make_zero"]);
    cmd
}

/// Exercises the exact production packet-copy recorder against a bounded
/// amount of real camera media without retaining the resulting fMP4.
pub async fn probe_recording_pipeline(
    config: &RecorderConfig,
    seconds: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !(1..=300).contains(&seconds) {
        return Err("recording probe duration must be 1..300 seconds".into());
    }
    let mut cmd = recording_ffmpeg_command(config);
    let mut child = cmd
        .args(["-t", &seconds.to_string()])
        .arg("pipe:1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut stderr = child.stderr.take().expect("piped stderr");
    let diagnostics = tokio::spawn(async move {
        let mut buffer = [0; 1024];
        let mut suffix = Vec::new();
        let mut bad_clock = false;
        loop {
            let n = stderr.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            suffix.extend_from_slice(&buffer[..n]);
            for needle in [
                b"Timestamps are unset".as_slice(),
                b"Non-monotonic DTS".as_slice(),
            ] {
                bad_clock |= suffix.windows(needle.len()).any(|window| window == needle);
            }
            if suffix.len() > 32 {
                suffix.drain(..suffix.len() - 32);
            }
        }
        Ok::<_, std::io::Error>(bad_clock)
    });
    let status = tokio::time::timeout(
        std::time::Duration::from_secs(u64::from(seconds) + 15),
        child.wait(),
    )
    .await;
    if status.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
    let bad_clock = diagnostics.await??;
    let status = status.map_err(|_| "recording probe deadline")??;
    if !status.success() {
        return Err(format!("recording packet-copy probe failed ({status})").into());
    }
    if bad_clock {
        return Err("recording packet-copy probe reported invalid clocks".into());
    }
    Ok(())
}

#[derive(Default)]
struct Lifetime(std::sync::Mutex<Option<Lease>>);
impl Drop for Lifetime {
    fn drop(&mut self) {
        if let Some(lease) = self.0.get_mut().unwrap().take() {
            lease.revoke();
        }
    }
}
#[derive(Clone)]
pub struct Recorder {
    lifetime: Arc<Lifetime>,
    state: Arc<Mutex<RecorderState>>,
    operation: Arc<Mutex<()>>,
    live_tx: broadcast::Sender<Fragment>,
    metrics: Arc<Metrics>,
}

impl Recorder {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        let (live_tx, _) = broadcast::channel(4);
        Self {
            lifetime: Arc::new(Lifetime::default()),
            state: Arc::new(Mutex::new(RecorderState::default())),
            operation: Arc::new(Mutex::new(())),
            live_tx,
            metrics,
        }
    }

    /// Serialized replacement: retire all escaped output before joining the old owner.
    pub async fn start(&self, config: RecorderConfig) {
        let _operation = self.operation.lock().await;
        {
            let state = self.state.lock().await;
            if state.lease.as_ref().is_some_and(Lease::current)
                && state.config.as_ref() == Some(&config)
            {
                return;
            }
        }
        self.stop_inner().await;
        let mut cmd = recording_ffmpeg_command(&config);
        cmd.arg("pipe:1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(error) => {
                self.metrics.error(ErrorSubsystem::Recording);
                error!("failed to start recorder: {error}");
                return;
            }
        };
        let stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        let lease = Lease::new();
        let mut state = self.state.lock().await;
        state.config = Some(config.clone());
        state.generation += 1;
        let generation = state.generation;
        state.lease = Some(lease.clone());
        *self.lifetime.0.lock().unwrap() = Some(lease.clone());
        let parser_state = self.state.clone();
        let live = self.live_tx.clone();
        let metrics = self.metrics.clone();
        state.owner = Some(tokio::spawn(async move {
            let diagnostics = tokio::spawn(async move {
                let mut bytes = [0; 1024];
                while matches!(stderr.read(&mut bytes).await, Ok(n) if n != 0) {}
            });
            let result = tokio::select! {
                biased;
                _ = lease.retired() => Ok(()),
                result = Self::parse_stream(parser_state, live, metrics.clone(), stdout, generation, config.prebuffer_ms, &lease) => result,
            };
            if result.is_err() && lease.current() {
                metrics.error(ErrorSubsystem::Recording);
                warn!("recording producer ended before stop");
            }
            lease.revoke();
            let _ = child.kill().await;
            let _ = child.wait().await;
            let _ = diagnostics.await;
        }));
    }
    pub async fn stop(&self) {
        let _operation = self.operation.lock().await;
        self.stop_inner().await;
    }
    async fn stop_inner(&self) {
        if let Some(lease) = self.lifetime.0.lock().unwrap().take() {
            lease.revoke();
        }
        let owner = {
            let mut state = self.state.lock().await;
            if let Some(lease) = state.lease.take() {
                lease.revoke();
            }
            state.generation += 1;
            state.config = None;
            state.init_segment = None;
            state.prebuffer.clear();
            state.owner.take()
        };
        if let Some(owner) = owner {
            let _ = owner.await;
        }
    }
    pub async fn is_running(&self) -> bool {
        self.state
            .lock()
            .await
            .lease
            .as_ref()
            .is_some_and(Lease::current)
    }

    /// Returns the init segment and current prebuffer contents, plus a live
    /// subscription for fragments produced from now on.
    pub async fn snapshot_and_subscribe(
        &self,
    ) -> (
        Option<Fragment>,
        Vec<Fragment>,
        broadcast::Receiver<Fragment>,
    ) {
        let state = self.state.lock().await;
        (
            state.init_segment.clone(),
            state.prebuffer.iter().cloned().collect(),
            self.live_tx.subscribe(),
        )
    }

    /// Reads MP4 boxes from ffmpeg stdout, splitting into the init segment
    /// (everything before the first `moof`) and `moof`+`mdat` fragments.
    async fn parse_stream(
        shared_state: Arc<Mutex<RecorderState>>,
        live_tx: broadcast::Sender<Fragment>,
        metrics: Arc<Metrics>,
        mut stdout: tokio::process::ChildStdout,
        generation: u64,
        prebuffer_ms: u32,
        lease: &Lease,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut init: Vec<u8> = Vec::new();
        let mut init_done = false;
        let mut fragment: Vec<u8> = Vec::new();

        loop {
            let mut header = [0u8; 8];
            stdout.read_exact(&mut header).await?;
            let mut size = u32::from_be_bytes(header[..4].try_into().unwrap()) as u64;
            let box_type = [header[4], header[5], header[6], header[7]];

            let mut box_bytes: Vec<u8> = header.to_vec();
            if size == 1 {
                let mut large = [0u8; 8];
                stdout.read_exact(&mut large).await?;
                box_bytes.extend_from_slice(&large);
                size = u64::from_be_bytes(large);
            } else if size == 0 {
                return Err("box with size 0".into());
            }
            if size < box_bytes.len() as u64 {
                return Err(format!("invalid box size {size}").into());
            }

            if size > MAX_BOX as u64 {
                return Err("recording box budget exceeded".into());
            }
            let body_len = (size as usize) - box_bytes.len();
            let mut body = vec![0u8; body_len];
            stdout.read_exact(&mut body).await?;
            box_bytes.extend_from_slice(&body);

            match &box_type {
                b"moof" => {
                    if init.is_empty() || !fragment.is_empty() {
                        return Err("invalid recording fragment order".into());
                    }
                    init_done = true;
                    fragment = box_bytes;
                }
                b"mdat" if init_done => {
                    if fragment.is_empty() {
                        return Err("recording mdat without moof".into());
                    }
                    if fragment.len() + box_bytes.len() > MAX_FRAGMENT {
                        return Err("recording fragment budget exceeded".into());
                    }
                    fragment.extend_from_slice(&box_bytes);
                    let complete = Fragment {
                        data: Arc::new(std::mem::take(&mut fragment)),
                        at: Instant::now(),
                        lease: lease.clone(),
                    };

                    let mut state = shared_state.lock().await;
                    if state.generation != generation {
                        return Ok(()); // superseded by a restart
                    }
                    if state.init_segment.is_none() {
                        state.init_segment = Some(Fragment {
                            data: Arc::new(init.clone()),
                            at: Instant::now(),
                            lease: lease.clone(),
                        });
                        debug!("recorder: init segment ready ({} bytes)", init.len());
                    }
                    // Keep enough fragments to cover the prebuffer window.
                    state.prebuffer.push_back(complete.clone());
                    let cutoff = tokio::time::Duration::from_millis(prebuffer_ms as u64 + 1000);
                    while let Some(front) = state.prebuffer.front() {
                        if front.at.elapsed() > cutoff
                            || state.prebuffer.len() > 8
                            || state.prebuffer.iter().map(|f| f.data.len()).sum::<usize>()
                                > MAX_PREBUFFER
                        {
                            state.prebuffer.pop_front();
                        } else {
                            break;
                        }
                    }
                    // Publication and snapshot+subscribe share this lock: no duplicate
                    // boundary fragment or lost historical/live transition.
                    metrics.recording_fragment(complete.data.len());
                    let _ = live_tx.send(complete);
                    drop(state);
                }
                _ if !init_done => {
                    if init.len() + box_bytes.len() > MAX_INIT {
                        return Err("recording initialization budget exceeded".into());
                    }
                    init.extend_from_slice(&box_bytes);
                }
                other => {
                    warn!(
                        "recorder: unexpected box {:?} mid-stream",
                        String::from_utf8_lossy(other)
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stop_joins_stalled_native_reader_and_revokes_escaped_epoch() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let recorder = Recorder::new(Metrics::new());
        recorder
            .start(RecorderConfig {
                rtsp_url: format!("rtsp://{}/stalled", listener.local_addr().unwrap()),
                audio: true,
                fps: 15,
                audio_sample_rate_hz: 48000,
                fragment_ms: 4000,
                prebuffer_ms: 4000,
            })
            .await;
        let lease = recorder.state.lock().await.lease.clone().unwrap();
        let (mut peer, _) =
            tokio::time::timeout(std::time::Duration::from_secs(3), listener.accept())
                .await
                .unwrap()
                .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), recorder.stop())
            .await
            .unwrap();
        recorder.stop().await;
        assert!(!lease.current());
        assert!(!recorder.is_running().await);
        assert!(recorder.state.lock().await.owner.is_none());
        let mut buffer = [0; 1024];
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while peer.read(&mut buffer).await.unwrap() != 0 {}
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn last_recorder_handle_drop_signals_native_owner() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let recorder = Recorder::new(Metrics::new());
        recorder
            .start(RecorderConfig {
                rtsp_url: format!("rtsp://{}/stalled", listener.local_addr().unwrap()),
                audio: false,
                fps: 15,
                audio_sample_rate_hz: 48000,
                fragment_ms: 4000,
                prebuffer_ms: 4000,
            })
            .await;
        let lease = recorder.state.lock().await.lease.clone().unwrap();
        let state = recorder.state.clone();
        let (mut peer, _) =
            tokio::time::timeout(std::time::Duration::from_secs(3), listener.accept())
                .await
                .unwrap()
                .unwrap();
        drop(recorder);
        assert!(!lease.current());
        let mut buffer = [0; 1024];
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while peer.read(&mut buffer).await.unwrap() != 0 {}
        })
        .await
        .unwrap();
        let owner = state.lock().await.owner.take().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), owner)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn timestamp_filters_build_monotonic_native_packet_clocks() {
        assert_eq!(
            video_timestamp_bitstream_filter(15),
            "setts=pts=N/(15*TB)+if(eq(PTS\\,NOPTS)\\,0\\,if(eq(DTS\\,NOPTS)\\,0\\,PTS-DTS)):dts=N/(15*TB):duration=1/(15*TB)"
        );
        assert!(video_timestamp_bitstream_filter(0).contains("N/(1*TB)"));
        assert_eq!(
            audio_timestamp_bitstream_filter(48_000),
            "setts=ts=N*1024/(48000*TB):duration=1024/(48000*TB)"
        );
    }
}
