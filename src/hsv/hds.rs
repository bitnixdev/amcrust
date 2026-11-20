//! HomeKit Data Stream server: TCP listener, session identification via trial
//! decryption, control/hello handshake, and the dataSend recording protocol.

use log::{debug, error, info, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, timeout};

use crate::hsv::codec::{self, Value};
use crate::hsv::frame::{self, SessionKeys};
use crate::hsv::lease::Lease;
use crate::hsv::recording::HsvState;
use crate::metrics::{ErrorSubsystem, Metrics, RecordingDeliveryOutcome};

#[cfg(test)]
#[path = "hds_tests.rs"]
mod tests;

const PREPARED_SESSION_TTL: Duration = Duration::from_secs(10);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
// TCP recording replay shares the network with latency-sensitive live SRTP.
// Send at most one small chunk at a time, even when the prebuffer is ready in bulk.
const MAX_CHUNK: usize = 64 * 1024;
const RECORDING_BYTES_PER_SECOND: u64 = 4 * 1024 * 1024;
const RECORDING_TAIL: Duration = Duration::from_secs(8);

struct PreparedSession {
    keys: SessionKeys,
    created: Instant,
}

struct Listener {
    port: u16,
    stop: Lease,
    task: tokio::task::JoinHandle<()>,
}

struct Inner {
    listener: Option<Listener>,
    prepared: Vec<PreparedSession>,
    stopped: bool,
}

/// One HDS server per accessory. The TCP listener is bound lazily on the first
/// SetupDataStreamTransport write and shared by all sessions.
#[derive(Clone)]
pub struct HdsServer {
    camera_name: Arc<str>,
    port: u16,
    inner: Arc<Mutex<Inner>>,
    // Keep the draining join handle reachable if a shutdown wait is cancelled.
    draining: Arc<Mutex<Option<Listener>>>,
    state: Arc<HsvState>,
    /// True while a dataSend recording stream is running (one at a time).
    recording_busy: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
}

impl HdsServer {
    pub fn new(
        camera_name: String,
        port: u16,
        state: Arc<HsvState>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            camera_name: camera_name.into(),
            port,
            inner: Arc::new(Mutex::new(Inner {
                listener: None,
                prepared: Vec::new(),
                stopped: false,
            })),
            draining: Arc::new(Mutex::new(None)),
            state,
            recording_busy: Arc::new(AtomicBool::new(false)),
            metrics,
        }
    }

    /// Handles a SetupDataStreamTransport write: derives session keys and
    /// returns (listening port, accessory key salt).
    pub async fn setup(
        &self,
        shared_secret: &[u8; 32],
        controller_salt: &[u8],
    ) -> Result<(u16, [u8; 32]), String> {
        let accessory_salt: [u8; 32] = rand::random();
        let keys = frame::derive_keys(shared_secret, controller_salt, &accessory_salt);

        let mut inner = self.inner.lock().await;
        inner
            .prepared
            .retain(|p| p.created.elapsed() < PREPARED_SESSION_TTL);
        if inner.stopped || inner.prepared.len() >= 16 {
            return Err("HDS admission closed".into());
        }
        let port = match &inner.listener {
            Some(listener) => listener.port,
            None => {
                let listener = TcpListener::bind(("0.0.0.0", self.port))
                    .await
                    .map_err(|e| e.to_string())?;
                let port = listener.local_addr().map_err(|e| e.to_string())?.port();
                info!("HDS listener bound on port {port}");
                let stop = Lease::new();
                let owner_stop = stop.clone();
                let server = self.clone();
                let task = tokio::spawn(async move {
                    let mut connections = tokio::task::JoinSet::new();
                    loop {
                        tokio::select! {
                            biased;
                            _ = owner_stop.retired() => break,
                            _ = connections.join_next(), if !connections.is_empty() => (),
                            accepted = listener.accept() => match accepted {
                                Ok((stream, _)) if connections.len() < 8 => {
                                    let server = server.clone();
                                    let stop = owner_stop.clone();
                                    server.metrics.data_stream_opened();
                                    connections.spawn(async move {
                                        if let Err(error) = server.handle_connection(stream, &stop).await {
                                            server.metrics.error(ErrorSubsystem::DataStream);
                                            debug!("HDS connection ended: {error}");
                                        }
                                        server.metrics.data_stream_closed();
                                    });
                                },
                                Ok(_) => (),
                                Err(error) => { server.metrics.error(ErrorSubsystem::DataStream); error!("HDS accept error: {error}"); break; }
                            }
                        }
                    }
                    owner_stop.revoke();
                    while connections.join_next().await.is_some() {}
                });
                inner.listener = Some(Listener { port, stop, task });
                port
            }
        };

        inner.prepared.push(PreparedSession {
            keys,
            created: Instant::now(),
        });
        Ok((port, accessory_salt))
    }

    pub async fn shutdown(&self) {
        let mut draining = self.draining.lock().await;
        if draining.is_none() {
            let mut inner = self.inner.lock().await;
            inner.stopped = true;
            inner.prepared.clear();
            if let Some(listener) = &inner.listener {
                listener.stop.revoke();
            }
            *draining = inner.listener.take();
        }
        if let Some(listener) = draining.as_mut() {
            if let Err(error) = (&mut listener.task).await {
                error!("HDS listener join failed: {error}");
            }
        }
        draining.take();
    }

    async fn handle_connection(&self, mut stream: TcpStream, stop: &Lease) -> Result<(), String> {
        stream.set_nodelay(true).ok();

        let mut buf: Vec<u8> = Vec::new();
        let mut read_buf = [0u8; 8192];

        // Read the first complete frame and identify the session by trial
        // decryption against all prepared sessions.
        let deadline = Instant::now() + HELLO_TIMEOUT;
        let (keys, first_payload) = loop {
            let complete = frame::complete_frame_len(&buf)?;
            if let Some(frame_len) = complete {
                let frame_bytes = &buf[..frame_len];
                let mut inner = self.inner.lock().await;
                inner
                    .prepared
                    .retain(|p| p.created.elapsed() < PREPARED_SESSION_TTL);
                let mut matched = None;
                for (i, prepared) in inner.prepared.iter().enumerate() {
                    let mut counter = 0u64;
                    if let Ok(payload) = frame::decrypt_frame(
                        &prepared.keys.controller_to_accessory,
                        &mut counter,
                        frame_bytes,
                    ) {
                        matched = Some((i, payload));
                        break;
                    }
                }
                match matched {
                    Some((i, payload)) => {
                        let prepared = inner.prepared.remove(i);
                        buf.drain(..frame_len);
                        break (prepared.keys, payload);
                    }
                    None => return Err("no prepared session matched first frame".into()),
                }
            }

            let read = timeout(
                deadline.saturating_duration_since(Instant::now()),
                stream.read(&mut read_buf),
            );
            let n = tokio::select! { biased; _ = stop.retired() => return Ok(()), result = read => result }
            .map_err(|_| "timed out waiting for first frame".to_string())
            .and_then(|r| r.map_err(|e| e.to_string()))?;
            if n == 0 {
                return Err("connection closed before identification".into());
            }
            buf.extend_from_slice(&read_buf[..n]);
        };

        let (read_half, write_half) = stream.into_split();
        let writer = Arc::new(Mutex::new(FrameWriter {
            write_half,
            key: keys.accessory_to_controller,
            counter: 0,
            poisoned: false,
            next_recording_chunk: Instant::now(),
        }));

        let mut conn = Connection {
            server: self.clone(),
            writer,
            read_key: keys.controller_to_accessory,
            read_counter: 1, // first frame consumed during identification
            hello_done: false,
            active_stream: None,
        };

        let run = async {
            conn.handle_payload(first_payload).await?;
            let mut read_half = read_half;
            loop {
                while let Some(frame_len) = frame::complete_frame_len(&buf)? {
                    let payload = frame::decrypt_frame(
                        &conn.read_key,
                        &mut conn.read_counter,
                        &buf[..frame_len],
                    )?;
                    buf.drain(..frame_len);
                    conn.handle_payload(payload).await?;
                }
                let n = read_half
                    .read(&mut read_buf)
                    .await
                    .map_err(|e| e.to_string())?;
                if n == 0 {
                    return Ok(());
                }
                buf.extend_from_slice(&read_buf[..n]);
            }
        };
        let result =
            tokio::select! { biased; _ = stop.retired() => Ok(()), result = run => result };
        conn.stop_active_stream().await;
        result
    }
}

struct FrameWriter {
    write_half: tokio::net::tcp::OwnedWriteHalf,
    key: [u8; 32],
    counter: u64,
    // Set before awaiting a frame write; cancellation leaves this sticky so no
    // later frame can be appended after a truncated encrypted frame.
    poisoned: bool,
    next_recording_chunk: Instant,
}

impl FrameWriter {
    async fn send_recording_chunk(
        &mut self,
        header: &Value,
        message: &Value,
        bytes: usize,
        leases: (&Lease, &Lease),
    ) -> Result<(), String> {
        tokio::select! {
            biased;
            _ = leases.0.retired() => return Err("recording output retired".into()),
            _ = leases.1.retired() => return Err("recording output retired".into()),
            _ = tokio::time::sleep_until(self.next_recording_chunk) => (),
        }
        self.send_fenced(header, message, Some(leases)).await?;
        // Charge each chunk after the socket accepts it. A slow socket cannot
        // accrue credit and send a catch-up burst later or at a fragment boundary.
        self.next_recording_chunk = Instant::now()
            + Duration::from_nanos(
                (bytes as u64 * 1_000_000_000).div_ceil(RECORDING_BYTES_PER_SECOND),
            );
        Ok(())
    }

    async fn send(&mut self, header: &Value, message: &Value) -> Result<(), String> {
        self.send_fenced(header, message, None).await
    }
    async fn send_fenced(
        &mut self,
        header: &Value,
        message: &Value,
        leases: Option<(&Lease, &Lease)>,
    ) -> Result<(), String> {
        if self.poisoned {
            return Err("HDS writer closed after incomplete frame".into());
        }
        let header_bytes = codec::encode_to_vec(header);
        let message_bytes = codec::encode_to_vec(message);
        if header_bytes.len() > 255 {
            return Err("HDS header too long".into());
        }
        let mut payload = Vec::with_capacity(1 + header_bytes.len() + message_bytes.len());
        payload.push(header_bytes.len() as u8);
        payload.extend_from_slice(&header_bytes);
        payload.extend_from_slice(&message_bytes);

        let frame = frame::encrypt_frame(&self.key, &mut self.counter, &payload)?;
        self.poisoned = true;
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut offset = 0;
        while offset < frame.len() {
            tokio::select! {
                biased;
                _ = async { match leases {
                    Some((epoch, stream)) => { tokio::select! { _ = epoch.retired() => (), _ = stream.retired() => () } },
                    None => std::future::pending::<()>().await,
                }} => return Err("recording output retired".into()),
                _ = tokio::time::sleep_until(deadline) => return Err("HDS write deadline".into()),
                ready = self.write_half.writable() => {
                    ready.map_err(|e| e.to_string())?;
                    let write = || self.write_half.try_write(&frame[offset..]);
                    let result = match leases {
                        Some((epoch, stream)) => epoch.write(|| stream.write(write))??,
                        None => write(),
                    };
                    match result {
                        Ok(0) => return Err("HDS write closed".into()),
                        Ok(n) => offset += n,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => (),
                        Err(error) => return Err(error.to_string()),
                    }
                }
            }
        }
        self.poisoned = false;
        Ok(())
    }
}

struct Connection {
    server: HdsServer,
    writer: Arc<Mutex<FrameWriter>>,
    read_key: [u8; 32],
    read_counter: u64,
    hello_done: bool,
    /// (streamId, cancel flag) of the running dataSend stream on this connection.
    active_stream: Option<ActiveStream>,
}

struct ActiveStream {
    id: i64,
    cancel: Lease,
    task: tokio::task::JoinHandle<()>,
}
struct Busy {
    flag: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
}
impl Drop for Busy {
    fn drop(&mut self) {
        self.metrics.recording_stream_active(false);
        self.flag.store(false, Ordering::SeqCst);
    }
}
impl Connection {
    async fn stop_active_stream(&mut self) {
        if let Some(active) = self.active_stream.take() {
            active.cancel.revoke();
            let _ = active.task.await;
        }
    }

    async fn handle_payload(&mut self, payload: Vec<u8>) -> Result<(), String> {
        if payload.is_empty() {
            return Err("empty HDS payload".into());
        }
        let header_len = payload[0] as usize;
        if payload.len() < 1 + header_len {
            return Err("truncated HDS header".into());
        }
        let mut decoder = codec::Decoder::new(&payload[1..]);
        let header = decoder
            .decode()
            .map_err(|e| e.to_string())?
            .ok_or("missing header dict")?;
        let message = codec::Decoder::new(&payload[1 + header_len..])
            .decode()
            .map_err(|e| e.to_string())?
            .unwrap_or(Value::Dict(vec![]));

        let protocol = header
            .get("protocol")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        debug!("HDS message: header {header:?}");

        if !self.hello_done {
            let is_hello = protocol == "control"
                && header.get("request").and_then(|v| v.as_str()) == Some("hello");
            if !is_hello {
                return Err("first HDS message was not control.hello".into());
            }
            let id = header.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
            let response_header = Value::dict(vec![
                ("protocol", Value::String("control".into())),
                ("response", Value::String("hello".into())),
                ("id", Value::Int64(id)),
                ("status", Value::Int64(0)),
            ]);
            self.writer
                .lock()
                .await
                .send(&response_header, &Value::Dict(vec![]))
                .await?;
            self.hello_done = true;
            debug!("HDS session established");
            return Ok(());
        }

        match protocol {
            "dataSend" => self.handle_data_send(&header, &message).await,
            other => {
                debug!("ignoring HDS protocol {other:?}");
                Ok(())
            }
        }
    }

    async fn handle_data_send(&mut self, header: &Value, message: &Value) -> Result<(), String> {
        if let Some(topic) = header.get("request").and_then(|v| v.as_str()) {
            let id = header.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
            match topic {
                "open" => self.handle_open(id, message).await,
                other => {
                    warn!("unknown dataSend request {other:?}");
                    self.respond(id, 4, Value::Dict(vec![])).await
                }
            }
        } else if let Some(topic) = header.get("event").and_then(|v| v.as_str()) {
            match topic {
                "close" => {
                    let stream_id = message.get("streamId").and_then(|v| v.as_i64());
                    debug!("dataSend close for stream {stream_id:?}");
                    if let Some(active) = &self.active_stream {
                        if stream_id == Some(active.id) {
                            self.stop_active_stream().await;
                        }
                    }
                    Ok(())
                }
                "ack" => {
                    debug!("dataSend ack: {message:?}");
                    if self.active_stream.as_ref().is_some_and(|active| {
                        message.get("streamId").and_then(|v| v.as_i64()) == Some(active.id)
                    }) {
                        self.stop_active_stream().await;
                    }
                    Ok(())
                }
                other => {
                    debug!("ignoring dataSend event {other:?}");
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }

    async fn respond(&self, id: i64, status: i64, message: Value) -> Result<(), String> {
        let header = Value::dict(vec![
            ("protocol", Value::String("dataSend".into())),
            ("response", Value::String("open".into())),
            ("id", Value::Int64(id)),
            ("status", Value::Int64(status)),
        ]);
        self.writer.lock().await.send(&header, &message).await
    }

    async fn handle_open(&mut self, id: i64, message: &Value) -> Result<(), String> {
        let stream_type = message.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let stream_id = message
            .get("streamId")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        if stream_type != "ipcamera.recording" {
            warn!("dataSend open for unsupported type {stream_type:?}");
            return self
                .respond(id, 6, Value::dict(vec![("status", Value::Int64(5))]))
                .await;
        }
        if let Err(reason) = self.server.state.recording_allowed().await {
            warn!("dataSend open rejected (reason {reason})");
            return self
                .respond(
                    id,
                    6,
                    Value::dict(vec![("status", Value::Int64(reason as i64))]),
                )
                .await;
        }
        if self
            .active_stream
            .as_ref()
            .is_some_and(|active| !active.cancel.current() || active.task.is_finished())
        {
            self.stop_active_stream().await;
        }
        if self.active_stream.is_some() || self.server.recording_busy.swap(true, Ordering::SeqCst) {
            warn!("dataSend open rejected: stream already running");
            return self
                .respond(id, 6, Value::dict(vec![("status", Value::Int64(2))]))
                .await;
        }
        let busy = Busy {
            flag: self.server.recording_busy.clone(),
            metrics: self.server.metrics.clone(),
        };
        self.server.metrics.recording_stream_active(true);

        self.respond(id, 0, Value::dict(vec![("status", Value::Int64(0))]))
            .await?;
        info!(
            "[{}] HomeKit recording started (stream {stream_id})",
            self.server.camera_name
        );

        let cancel = Lease::new();
        let task_cancel = cancel.clone();

        let writer = self.writer.clone();
        let state = self.server.state.clone();
        let metrics = self.server.metrics.clone();
        let camera_name = self.server.camera_name.clone();
        let task = tokio::spawn(async move {
            let _busy = busy;
            let output = writer.clone();
            let result = pump_recording(writer, state, stream_id, task_cancel.clone()).await;
            task_cancel.revoke();
            if !matches!(result, Ok(RecordingDeliveryOutcome::EndSent)) {
                let mut output = output.lock().await;
                output.poisoned = true;
                let _ = output.write_half.shutdown().await;
            }
            let outcome = match result {
                Ok(outcome) => outcome,
                Err(e) => {
                    metrics.error(ErrorSubsystem::DataStream);
                    warn!("recording stream {stream_id} ended with error: {e}");
                    RecordingDeliveryOutcome::WriteError
                }
            };
            metrics.recording_delivery_finished(outcome);
            info!("[{camera_name}] HomeKit recording stopped (stream {stream_id})");
        });
        self.active_stream = Some(ActiveStream {
            id: stream_id,
            cancel,
            task,
        });

        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(active) = &self.active_stream {
            active.cancel.revoke();
        }
    }
}

/// Sends the recording (init + prebuffer + live fragments) as dataSend data
/// events until motion stops, a short post-event tail is delivered, or the
/// controller closes the stream.
async fn pump_recording(
    writer: Arc<Mutex<FrameWriter>>,
    state: Arc<HsvState>,
    stream_id: i64,
    cancel: Lease,
) -> Result<RecordingDeliveryOutcome, String> {
    use super::fmp4::Message;
    let mut clip = match state.recorder.open().await {
        Ok(clip) => clip,
        Err(error) => {
            debug!("recording stream {stream_id} unavailable: {error}");
            send_close(&writer, stream_id, 6).await?;
            return Ok(RecordingDeliveryOutcome::SourceUnavailable);
        }
    };
    let lease = clip.lease.clone();
    let pump = async {
        let mut sequence = 1;
        let mut initialized = false;
        let mut pending: Option<super::fmp4::Fragment> = None;
        let mut inactive_since: Option<Instant> = None;
        let mut finishing = false;
        let started = Instant::now();
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        let mut deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let message = tokio::select! {
                _ = tick.tick() => {
                    if !finishing && started.elapsed() >= Duration::from_secs(60) {
                        finishing = true;
                        clip.finish();
                    }
                    if !finishing {
                        if state.motion_active.load(Ordering::SeqCst) { inactive_since = None; }
                        else if inactive_since.get_or_insert_with(Instant::now).elapsed() >= RECORDING_TAIL {
                            finishing = true;
                            clip.finish();
                        }
                    }
                    continue;
                },
                _ = tokio::time::sleep_until(deadline) => return Err("recording mux deadline".into()),
                result = clip.messages.recv() => result.ok_or("recording mux ended without completion")?,
            };
            deadline = Instant::now() + Duration::from_secs(30);
            match message {
                Message::Init(fragment) => {
                    if initialized {
                        return Err("repeated recording init".into());
                    }
                    initialized = true;
                    send_data(
                        &writer,
                        stream_id,
                        &fragment.data,
                        "mediaInitialization",
                        sequence,
                        false,
                        (&fragment.lease, &cancel),
                    )
                    .await?;
                    sequence += 1;
                }
                Message::Media(fragment) => {
                    if !initialized {
                        return Err("recording media before init".into());
                    }
                    // Retain one complete fragment so only a successful finite
                    // mux completion can mark the final HDS packet.
                    if let Some(previous) = pending.replace(fragment) {
                        send_data(
                            &writer,
                            stream_id,
                            &previous.data,
                            "mediaFragment",
                            sequence,
                            false,
                            (&previous.lease, &cancel),
                        )
                        .await?;
                        sequence += 1;
                    }
                }
                Message::End => {
                    let final_fragment = pending.take().ok_or("empty completed recording")?;
                    send_data(
                        &writer,
                        stream_id,
                        &final_fragment.data,
                        "mediaFragment",
                        sequence,
                        true,
                        (&final_fragment.lease, &cancel),
                    )
                    .await?;
                    return Ok(RecordingDeliveryOutcome::EndSent);
                }
            }
        }
    };
    let result = tokio::select! {
        biased;
        _ = cancel.retired() => Ok(RecordingDeliveryOutcome::Cancelled),
        _ = lease.retired() => Ok(RecordingDeliveryOutcome::SourceUnavailable),
        result = pump => result,
    };
    // Do not cancel this owner from an outer select: stop always joins its mux.
    clip.stop().await;
    result
}

async fn send_close(
    writer: &Arc<Mutex<FrameWriter>>,
    stream_id: i64,
    reason: i64,
) -> Result<(), String> {
    let header = Value::dict(vec![
        ("protocol", Value::String("dataSend".into())),
        ("event", Value::String("close".into())),
    ]);
    let message = Value::dict(vec![
        ("streamId", Value::Int64(stream_id)),
        ("reason", Value::Int64(reason)),
    ]);
    writer.lock().await.send(&header, &message).await
}

async fn send_data(
    writer: &Arc<Mutex<FrameWriter>>,
    stream_id: i64,
    data: &[u8],
    data_type: &str,
    sequence: u64,
    end_of_stream: bool,
    leases: (&Lease, &Lease),
) -> Result<(), String> {
    let total = data.len();
    let chunks: Vec<&[u8]> = if data.is_empty() {
        vec![&[]]
    } else {
        data.chunks(MAX_CHUNK).collect()
    };
    let count = chunks.len();

    for (i, chunk) in chunks.into_iter().enumerate() {
        let is_last = i + 1 == count;
        let mut metadata = vec![
            ("dataType", Value::String(data_type.into())),
            ("dataSequenceNumber", Value::Int(sequence as i64)),
            ("dataChunkSequenceNumber", Value::Int((i + 1) as i64)),
            ("isLastDataChunk", Value::Bool(is_last)),
        ];
        if i == 0 {
            metadata.push(("dataTotalSize", Value::Int(total as i64)));
        }
        let packet = Value::dict(vec![
            ("data", Value::Data(chunk.to_vec())),
            (
                "metadata",
                Value::Dict(
                    metadata
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect(),
                ),
            ),
        ]);

        let mut message = vec![
            ("streamId".to_string(), Value::Int64(stream_id)),
            ("packets".to_string(), Value::Array(vec![packet])),
        ];
        if is_last {
            message.push(("endOfStream".to_string(), Value::Bool(end_of_stream)));
        }

        let header = Value::dict(vec![
            ("protocol", Value::String("dataSend".into())),
            ("event", Value::String("data".into())),
        ]);
        writer
            .lock()
            .await
            .send_recording_chunk(&header, &Value::Dict(message), chunk.len(), leases)
            .await?;
    }
    Ok(())
}
