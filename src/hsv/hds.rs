//! HomeKit Data Stream server: TCP listener, session identification via trial
//! decryption, control/hello handshake, and the dataSend recording protocol.

use log::{debug, error, info, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, timeout};

use crate::hsv::codec::{self, Value};
use crate::hsv::frame::{self, SessionKeys};
use crate::hsv::recording::HsvState;

const PREPARED_SESSION_TTL: Duration = Duration::from_secs(10);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CHUNK: usize = 0x40000;
const RECORDING_TAIL: Duration = Duration::from_secs(8);

struct PreparedSession {
    keys: SessionKeys,
    created: Instant,
}

struct Listener {
    port: u16,
}

struct Inner {
    listener: Option<Listener>,
    prepared: Vec<PreparedSession>,
}

/// One HDS server per accessory. The TCP listener is bound lazily on the first
/// SetupDataStreamTransport write and shared by all sessions.
#[derive(Clone)]
pub struct HdsServer {
    inner: Arc<Mutex<Inner>>,
    state: Arc<HsvState>,
    /// True while a dataSend recording stream is running (one at a time).
    recording_busy: Arc<AtomicBool>,
}

impl HdsServer {
    pub fn new(state: Arc<HsvState>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                listener: None,
                prepared: Vec::new(),
            })),
            state,
            recording_busy: Arc::new(AtomicBool::new(false)),
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
        inner.prepared.push(PreparedSession {
            keys,
            created: Instant::now(),
        });

        let port = match &inner.listener {
            Some(listener) => listener.port,
            None => {
                let listener = TcpListener::bind("0.0.0.0:0")
                    .await
                    .map_err(|e| e.to_string())?;
                let port = listener.local_addr().map_err(|e| e.to_string())?.port();
                info!("HDS listener bound on port {port}");
                inner.listener = Some(Listener { port });

                let server = self.clone();
                tokio::spawn(async move {
                    loop {
                        match listener.accept().await {
                            Ok((stream, addr)) => {
                                info!("HDS connection from {addr}");
                                let server = server.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = server.handle_connection(stream).await {
                                        info!("HDS connection ended: {e}");
                                    }
                                });
                            }
                            Err(e) => {
                                error!("HDS accept error: {e}");
                                break;
                            }
                        }
                    }
                });
                port
            }
        };

        Ok((port, accessory_salt))
    }

    async fn handle_connection(&self, mut stream: TcpStream) -> Result<(), String> {
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

            let n = timeout(
                deadline.saturating_duration_since(Instant::now()),
                stream.read(&mut read_buf),
            )
            .await
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
        }));

        let mut conn = Connection {
            server: self.clone(),
            writer,
            read_key: keys.controller_to_accessory,
            read_counter: 1, // first frame consumed during identification
            hello_done: false,
            active_stream: None,
        };

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
                conn.stop_active_stream();
                return Ok(());
            }
            buf.extend_from_slice(&read_buf[..n]);
        }
    }
}

struct FrameWriter {
    write_half: tokio::net::tcp::OwnedWriteHalf,
    key: [u8; 32],
    counter: u64,
}

impl FrameWriter {
    async fn send(&mut self, header: &Value, message: &Value) -> Result<(), String> {
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
        self.write_half
            .write_all(&frame)
            .await
            .map_err(|e| e.to_string())
    }
}

struct Connection {
    server: HdsServer,
    writer: Arc<Mutex<FrameWriter>>,
    read_key: [u8; 32],
    read_counter: u64,
    hello_done: bool,
    /// (streamId, cancel flag) of the running dataSend stream on this connection.
    active_stream: Option<(i64, Arc<AtomicBool>)>,
}

impl Connection {
    fn stop_active_stream(&mut self) {
        if let Some((_, cancel)) = self.active_stream.take() {
            cancel.store(true, Ordering::SeqCst);
        }
        self.server.recording_busy.store(false, Ordering::SeqCst);
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
            info!("HDS session established");
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
                    info!("dataSend close for stream {stream_id:?}");
                    if let Some((active_id, _)) = &self.active_stream {
                        if stream_id == Some(*active_id) || stream_id.is_none() {
                            self.stop_active_stream();
                        }
                    }
                    Ok(())
                }
                "ack" => {
                    debug!("dataSend ack: {message:?}");
                    self.stop_active_stream();
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
        if self.server.recording_busy.swap(true, Ordering::SeqCst) {
            warn!("dataSend open rejected: stream already running");
            return self
                .respond(id, 6, Value::dict(vec![("status", Value::Int64(2))]))
                .await;
        }

        self.respond(id, 0, Value::dict(vec![("status", Value::Int64(0))]))
            .await?;
        info!("dataSend recording stream {stream_id} opened");

        let cancel = Arc::new(AtomicBool::new(false));
        self.active_stream = Some((stream_id, cancel.clone()));

        let writer = self.writer.clone();
        let state = self.server.state.clone();
        let busy = self.server.recording_busy.clone();
        tokio::spawn(async move {
            if let Err(e) = pump_recording(writer, state, stream_id, cancel).await {
                warn!("recording stream {stream_id} ended with error: {e}");
            }
            busy.store(false, Ordering::SeqCst);
        });

        Ok(())
    }
}

/// Sends the recording (init + prebuffer + live fragments) as dataSend data
/// events until motion stops, a short post-event tail is delivered, or the
/// controller closes the stream.
async fn pump_recording(
    writer: Arc<Mutex<FrameWriter>>,
    state: Arc<HsvState>,
    stream_id: i64,
    cancel: Arc<AtomicBool>,
) -> Result<(), String> {
    let (init, prebuffer, mut live) = state.recorder.snapshot_and_subscribe().await;
    let Some(init) = init else {
        return send_close(&writer, stream_id, 6).await; // TIMEOUT: nothing buffered yet
    };
    let prebuffer_fragments = prebuffer.len();
    let prebuffer_bytes: usize = prebuffer.iter().map(|fragment| fragment.data.len()).sum();
    info!(
        "recording stream {stream_id}: sending {}-byte init and {prebuffer_fragments} prebuffer fragments ({prebuffer_bytes} bytes)",
        init.len()
    );

    let sequence = AtomicU64::new(1);
    let mut fragment_count = 0usize;
    let mut media_bytes = 0usize;

    // Sequence 1: media initialization.
    send_data(
        &writer,
        stream_id,
        &init,
        "mediaInitialization",
        sequence.fetch_add(1, Ordering::SeqCst),
        false,
    )
    .await?;

    for fragment in prebuffer {
        if cancel.load(Ordering::SeqCst) {
            info!("recording stream {stream_id}: cancelled during prebuffer");
            return Ok(());
        }
        let seq = sequence.fetch_add(1, Ordering::SeqCst);
        send_data(
            &writer,
            stream_id,
            &fragment.data,
            "mediaFragment",
            seq,
            false,
        )
        .await?;
        fragment_count += 1;
        media_bytes += fragment.data.len();
    }

    let mut inactive_since: Option<Instant> = None;
    loop {
        if cancel.load(Ordering::SeqCst) {
            info!(
                "recording stream {stream_id}: controller closed after {fragment_count} fragments ({media_bytes} bytes)"
            );
            return Ok(());
        }
        let fragment = match timeout(Duration::from_secs(30), live.recv()).await {
            Ok(Ok(fragment)) => fragment,
            Ok(Err(_)) => return send_close(&writer, stream_id, 5).await,
            Err(_) => return send_close(&writer, stream_id, 6).await,
        };
        let motion_still_active = state.motion_active.load(Ordering::SeqCst);
        let end_of_stream = if motion_still_active {
            inactive_since = None;
            false
        } else {
            let stopped_at = inactive_since.get_or_insert_with(Instant::now);
            stopped_at.elapsed() >= RECORDING_TAIL
        };
        let seq = sequence.fetch_add(1, Ordering::SeqCst);
        send_data(
            &writer,
            stream_id,
            &fragment.data,
            "mediaFragment",
            seq,
            end_of_stream,
        )
        .await?;
        fragment_count += 1;
        media_bytes += fragment.data.len();
        if end_of_stream {
            info!(
                "recording stream {stream_id}: end of stream sent after {fragment_count} fragments ({media_bytes} bytes, {}s post-motion tail)",
                RECORDING_TAIL.as_secs()
            );
            return Ok(());
        }
    }
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
            .send(&header, &Value::Dict(message))
            .await?;
    }
    Ok(())
}
