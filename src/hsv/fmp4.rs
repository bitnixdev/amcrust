//! Fragmented-MP4 recording pipeline: a persistent ffmpeg process stream-copies
//! the camera's RTSP feed into fMP4 (init segment + keyframe-aligned
//! fragments), which we parse into segments and keep in a rolling prebuffer.

use log::{debug, error, warn};
use std::collections::VecDeque;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, broadcast};
use tokio::time::Instant;

use crate::metrics::{ErrorSubsystem, Metrics};

#[derive(Clone, Debug)]
pub struct Fragment {
    pub data: Arc<Vec<u8>>,
    pub at: Instant,
}

#[derive(Default)]
struct RecorderState {
    init_segment: Option<Arc<Vec<u8>>>,
    prebuffer: VecDeque<Fragment>,
    child: Option<Child>,
    generation: u64,
    config: Option<RecorderConfig>,
}

/// Encoder settings for the recording pipeline (already applied to the camera;
/// ffmpeg only stream-copies).
#[derive(Clone, Debug, PartialEq)]
pub struct RecorderConfig {
    pub rtsp_url: String,
    pub audio: bool,
    pub fragment_ms: u32,
    pub prebuffer_ms: u32,
}

#[derive(Clone)]
pub struct Recorder {
    state: Arc<Mutex<RecorderState>>,
    live_tx: broadcast::Sender<Fragment>,
    metrics: Arc<Metrics>,
}

impl Recorder {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        let (live_tx, _) = broadcast::channel(16);
        Self {
            state: Arc::new(Mutex::new(RecorderState::default())),
            live_tx,
            metrics,
        }
    }

    /// (Re)starts the pipeline with the given configuration. A no-op when the
    /// pipeline is already running with the same configuration.
    pub async fn start(&self, config: RecorderConfig) {
        let mut state = self.state.lock().await;
        if state.child.is_some() && state.config.as_ref() == Some(&config) {
            return;
        }
        state.config = Some(config.clone());
        state.generation += 1;
        let generation = state.generation;
        if let Some(mut child) = state.child.take() {
            let _ = child.start_kill();
        }
        state.init_segment = None;
        state.prebuffer.clear();

        let mut cmd = Command::new("ffmpeg");
        cmd.arg("-hide_banner")
            .args(["-loglevel", "warning"])
            .args(["-fflags", "+genpts"])
            // See stream.rs: the camera's timestamps wobble; arrival time is
            // the reliable clock.
            .args(["-use_wallclock_as_timestamps", "1"])
            .args(["-rtsp_transport", "tcp"])
            .args(["-i", &config.rtsp_url]);
        if config.audio {
            cmd.args(["-c:v", "copy"]).args(["-c:a", "copy"]);
        } else {
            cmd.arg("-an").args(["-c:v", "copy"]);
        }
        cmd.args(["-f", "mp4"])
            .args(["-movflags", "frag_keyframe+empty_moov+default_base_moof"])
            // `reset_timestamps` belongs to FFmpeg's segment muxer, not MP4.
            // Keep generated wall-clock timestamps and shift the whole output
            // timeline to zero without destroying packet timestamps.
            .args(["-avoid_negative_ts", "make_zero"])
            .arg("pipe:1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        debug!(
            "starting recording pipeline ({}ms fragments)",
            config.fragment_ms
        );
        match cmd.spawn() {
            Ok(mut child) => {
                if let Some(stderr) = child.stderr.take() {
                    tokio::spawn(async move {
                        use tokio::io::AsyncBufReadExt;
                        let mut lines = tokio::io::BufReader::new(stderr).lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            warn!("recorder ffmpeg: {line}");
                        }
                    });
                }
                let stdout = child.stdout.take().expect("piped stdout");
                state.child = Some(child);

                let recorder = self.clone();
                let prebuffer_ms = config.prebuffer_ms;
                tokio::spawn(async move {
                    if let Err(e) = recorder
                        .parse_stream(stdout, generation, prebuffer_ms)
                        .await
                    {
                        // Quiet when this pipeline was deliberately replaced.
                        if recorder.state.lock().await.generation == generation {
                            recorder.metrics.error(ErrorSubsystem::Recording);
                            error!("recorder stream ended: {e}");
                        }
                    }
                });
            }
            Err(e) => {
                self.metrics.error(ErrorSubsystem::Recording);
                error!("failed to spawn recorder ffmpeg: {e}");
            }
        }
    }

    pub async fn stop(&self) {
        let mut state = self.state.lock().await;
        state.generation += 1;
        state.config = None;
        if let Some(mut child) = state.child.take() {
            let _ = child.start_kill();
            debug!("recording pipeline stopped");
        }
        state.init_segment = None;
        state.prebuffer.clear();
    }

    pub async fn is_running(&self) -> bool {
        self.state.lock().await.child.is_some()
    }

    /// Returns the init segment and current prebuffer contents, plus a live
    /// subscription for fragments produced from now on.
    pub async fn snapshot_and_subscribe(
        &self,
    ) -> (
        Option<Arc<Vec<u8>>>,
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
        &self,
        mut stdout: tokio::process::ChildStdout,
        generation: u64,
        prebuffer_ms: u32,
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

            let body_len = (size as usize) - box_bytes.len();
            let mut body = vec![0u8; body_len];
            stdout.read_exact(&mut body).await?;
            box_bytes.extend_from_slice(&body);

            match &box_type {
                b"moof" => {
                    init_done = true;
                    fragment = box_bytes;
                }
                b"mdat" if init_done => {
                    fragment.extend_from_slice(&box_bytes);
                    let complete = Fragment {
                        data: Arc::new(std::mem::take(&mut fragment)),
                        at: Instant::now(),
                    };

                    let mut state = self.state.lock().await;
                    if state.generation != generation {
                        return Ok(()); // superseded by a restart
                    }
                    if state.init_segment.is_none() {
                        state.init_segment = Some(Arc::new(init.clone()));
                        debug!("recorder: init segment ready ({} bytes)", init.len());
                    }
                    // Keep enough fragments to cover the prebuffer window.
                    state.prebuffer.push_back(complete.clone());
                    let cutoff = tokio::time::Duration::from_millis(prebuffer_ms as u64 + 1000);
                    while let Some(front) = state.prebuffer.front() {
                        if front.at.elapsed() > cutoff || state.prebuffer.len() > 8 {
                            state.prebuffer.pop_front();
                        } else {
                            break;
                        }
                    }
                    drop(state);

                    self.metrics.recording_fragment(complete.data.len());
                    let _ = self.live_tx.send(complete);
                }
                _ if !init_done => init.extend_from_slice(&box_bytes),
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
