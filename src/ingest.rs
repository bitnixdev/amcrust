//! Single native RTSP acquisition per encoder; bounded, generation-scoped packet leases.
//! Consumers never receive camera URLs. Only sink-private IPC sequence counters are rebased.
use std::{path::PathBuf, process::Stdio, sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering}}, time::Duration};
use serde_json::Value;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, process::{Child, ChildStdin, Command}, sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore}, task::JoinHandle, time::{Instant, timeout}};

const BUDGET: usize = 4 * 1024 * 1024;
const RESIDENCE: Duration = Duration::from_secs(2);
type Error = Box<dyn std::error::Error + Send + Sync>;

pub fn binary(name: &str) -> PathBuf {
    let directory = std::env::var_os("AMCRUST_NATIVE_DIR").map(PathBuf::from)
        .or_else(|| option_env!("AMCRUST_NATIVE_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/native-media/bin"));
    directory.join(name)
}

struct Frame { metadata: Value, blob: Vec<u8>, born: Instant }
impl Frame {
    async fn read(reader: &mut (impl tokio::io::AsyncRead + Unpin)) -> Result<Arc<Self>, Error> {
        let size = reader.read_u32().await? as usize;
        if !(13..=6*1024*1024).contains(&size) { return Err("native frame size".into()); }
        let mut body = vec![0; size];
        reader.read_exact(&mut body).await?;
        if &body[..4] != b"AMC2" { return Err("native envelope".into()); }
        let meta = u32::from_be_bytes(body[4..8].try_into()?) as usize;
        let blob = u32::from_be_bytes(body[8..12].try_into()?) as usize;
        if meta == 0 || meta > 65536 || 12 + meta + blob != size { return Err("native frame layout".into()); }
        let metadata: Value = serde_json::from_slice(&body[12..12+meta])?;
        Ok(Arc::new(Self { metadata, blob: body[12+meta..].to_vec(), born: Instant::now() }))
    }
    fn bytes(&self, key: &str) -> Result<&[u8], Error> {
        let r = &self.metadata[key];
        let start = r["offset"].as_u64().ok_or("native byte offset")? as usize;
        let len = r["length"].as_u64().ok_or("native byte length")? as usize;
        self.blob.get(start..start.checked_add(len).ok_or("native range overflow")?).ok_or_else(|| "native byte range".into())
    }
    async fn write(&self, out: &mut ChildStdin, sequence: Option<u64>) -> Result<(), Error> {
        let mut metadata = self.metadata.clone();
        if let Some(n) = sequence { metadata["sequence"] = n.into(); }
        let json = serde_json::to_vec(&metadata)?;
        out.write_u32((12 + json.len() + self.blob.len()) as u32).await?;
        out.write_all(b"AMC2").await?;
        out.write_u32(json.len() as u32).await?;
        out.write_u32(self.blob.len() as u32).await?;
        out.write_all(&json).await?;
        out.write_all(&self.blob).await?;
        Ok(())
    }
}

fn nals(data: &[u8]) -> Vec<&[u8]> {
    let mut starts = Vec::new();
    let mut i = 0;
    while i + 3 <= data.len() {
        let len = if data[i..].starts_with(&[0,0,0,1]) {4} else if data[i..].starts_with(&[0,0,1]) {3} else { i+=1; continue; };
        starts.push((i, i+len)); i+=len;
    }
    starts.iter().enumerate().filter_map(|(n, &(_, start))| {
        let end = starts.get(n+1).map_or(data.len(), |v| v.0);
        (end > start).then_some(&data[start..end])
    }).collect()
}

struct Message { headers: Option<Vec<Arc<Frame>>>, frame: Arc<Frame>, _charge: OwnedSemaphorePermit }
struct Client { tx: mpsc::Sender<Message>, alive: Arc<AtomicBool>, budget: Arc<Semaphore>, started: bool, video: bool }
struct Lease { rx: mpsc::Receiver<Message>, alive: Arc<AtomicBool> }
impl Drop for Lease { fn drop(&mut self) { self.alive.store(false, Ordering::SeqCst); } }
enum Request { Join(Client), Stop }
struct Running { tx: mpsc::Sender<Request>, task: JoinHandle<()> }

pub struct Source {
    url: String,
    subtype: u8,
    enabled: AtomicBool,
    generation: AtomicU32,
    running: Mutex<Option<Running>>,
}
impl Source {
    pub fn new(url: String, subtype: u8) -> Arc<Self> {
        Arc::new(Self { url, subtype, enabled: AtomicBool::new(true), generation: AtomicU32::new(0), running: Mutex::new(None) })
    }
    pub async fn reset(&self) {
        let mut running = self.running.lock().await;
        if let Some(run) = running.take() {
            let _ = run.tx.send(Request::Stop).await;
            let _ = run.task.await;
        }
    }
    pub async fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
        if !enabled { self.reset().await; }
    }
    async fn subscribe(self: &Arc<Self>, video: bool) -> Result<Lease, Error> {
        let mut running = self.running.lock().await;
        if !self.enabled.load(Ordering::SeqCst) { return Err("camera acquisition disabled".into()); }
        if running.as_ref().is_some_and(|r| r.task.is_finished()) {
            let old = running.take().unwrap(); let _ = old.task.await;
        }
        if running.is_none() {
            let (tx, rx) = mpsc::channel(16);
            let source = self.clone();
            let task = tokio::spawn(async move { source.run(rx).await; });
            *running = Some(Running { tx, task });
        }
        let (tx, rx) = mpsc::channel(64);
        let alive = Arc::new(AtomicBool::new(true));
        running.as_ref().unwrap().tx.send(Request::Join(Client { tx, alive: alive.clone(), budget: Arc::new(Semaphore::new(BUDGET)), started: false, video })).await?;
        Ok(Lease { rx, alive })
    }
    async fn run(self: Arc<Self>, mut requests: mpsc::Receiver<Request>) {
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let mut clients: Vec<Client> = Vec::new();
        let Some(Request::Join(first)) = requests.recv().await else { return };
        clients.push(first);
        let mut command = Command::new(binary("amcrust-ingest"));
        command.args(["--camera", &generation.to_string()]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).kill_on_drop(true);
        let mut child = match command.spawn() { Ok(c) => c, Err(e) => {
            log::error!("cannot start native acquisition subtype {}: {e}", self.subtype);
            for c in clients { c.alive.store(false, Ordering::SeqCst); } return;
        }};
        log::info!("native acquisition started: subtype {}, generation {}, pid {:?}", self.subtype, generation, child.id());
        let result: Result<(), Error> = async {
            let mut input = child.stdin.take().ok_or("source stdin")?;
            timeout(Duration::from_secs(2), input.write_all(format!("{}\n", self.url).as_bytes())).await??;
            drop(input);
            let mut output = child.stdout.take().ok_or("source stdout")?;
            let mut headers: Vec<Arc<Frame>> = Vec::new();
            let mut expected = 0u64;
            let mut video_index = None;
            let mut audio_index = None;
            let mut parameter_sets: Vec<Vec<u8>> = Vec::new();
            let mut stream_count = 0usize;
            let mut last_dts = [None; 8];
            loop {
                let reading = timeout(Duration::from_secs(5), Frame::read(&mut output));
                tokio::pin!(reading);
                let frame = loop {
                    tokio::select! {
                        result = &mut reading => break result??,
                        request = requests.recv() => match request {
                            Some(Request::Join(client)) if clients.len() < 8 => clients.push(client),
                            Some(Request::Join(client)) => { client.alive.store(false, Ordering::SeqCst); },
                            Some(Request::Stop) | None => return Ok(()),
                        }
                    }
                };
                if frame.metadata["generation"].as_u64() != Some(generation as u64) { return Err("source generation mismatch".into()); }
                match frame.metadata["type"].as_str() {
                    Some("hello") if headers.is_empty() => {
                        stream_count = frame.metadata["streams"].as_u64().ok_or("stream count")? as usize;
                        if !(1..=8).contains(&stream_count) { return Err("source stream count".into()); }
                        headers.push(frame); continue;
                    }
                    Some("stream") if headers.len() <= stream_count => {
                        let index = frame.metadata["index"].as_u64().ok_or("stream index")? as usize;
                        if index + 1 != headers.len() { return Err("descriptor order".into()); }
                        match frame.metadata["codec"].as_str() {
                            Some("h264") if video_index.is_none() => {
                                video_index = Some(index);
                                parameter_sets = nals(frame.bytes("extradata")?).into_iter().filter(|n| matches!(n[0]&31,7|8)).map(Vec::from).collect();
                                if parameter_sets.len() != 2 { return Err("H264 SPS/PPS configuration required".into()); }
                            },
                            Some("aac") if audio_index.is_none() => audio_index = Some(index),
                            _ => return Err("unsupported camera codec layout".into()),
                        }
                        headers.push(frame); continue;
                    }
                    Some("packet") if headers.len() == stream_count + 1 => {},
                    _ => return Err("unexpected source lifecycle".into()),
                }
                if frame.metadata["sequence"].as_u64() != Some(expected) { return Err("source packet sequence gap".into()); }
                expected += 1;
                let index = frame.metadata["stream"].as_u64().ok_or("packet stream")? as usize;
                if index >= stream_count { return Err("packet stream range".into()); }
                let mut idr = false;
                if Some(index) == video_index {
                    for nal in nals(frame.bytes("data")?) {
                        if matches!(nal[0]&31,7|8) && !parameter_sets.iter().any(|p| p.as_slice() == nal) {
                            return Err("camera parameter sets changed".into());
                        }
                        idr |= nal[0]&31 == 5 && nal[0]&0x60 != 0;
                    }
                }
                // Never invent missing source clocks; wait for a subsequent timestamped IDR.
                let (Some(_pts), Some(dts)) = (frame.metadata["pts"].as_i64(), frame.metadata["dts"].as_i64()) else {
                    if clients.iter().any(|c| c.started) { return Err("source lost packet timestamps".into()); }
                    continue;
                };
                if last_dts[index].is_some_and(|last| dts <= last) { return Err("source decode clock regressed".into()); }
                last_dts[index] = Some(dts);
                clients.retain(|c| c.alive.load(Ordering::SeqCst) && !c.tx.is_closed());
                if clients.is_empty() { return Ok(()); }
                for client in &mut clients {
                    if !client.started && !(if client.video { idr && frame.metadata["flags"].as_u64().unwrap_or(0)&1 != 0 } else { Some(index)==audio_index }) { continue; }
                    let bytes = frame.blob.len() + 65536;
                    let charge = client.budget.clone().try_acquire_many_owned(bytes as u32);
                    match charge {
                        Ok(charge) => {
                            let message = Message { headers: (!client.started).then(|| headers.clone()), frame: frame.clone(), _charge: charge };
                            if client.tx.try_send(message).is_err() { client.alive.store(false, Ordering::SeqCst); }
                            else { client.started = true; }
                        },
                        Err(_) => { client.alive.store(false, Ordering::SeqCst); }
                    }
                }
            }
        }.await;
        for c in clients { c.alive.store(false, Ordering::SeqCst); }
        // Invalidate leases before waiting on OS cleanup; pipes contain bounded data.
        let _ = child.kill().await;
        let _ = child.wait().await;
        if let Err(error) = result { log::warn!("native source subtype {} generation {} retired: {error}", self.subtype, generation); }
        log::info!("native acquisition stopped: subtype {}, generation {}", self.subtype, generation);
    }
    pub async fn output(self: &Arc<Self>, args: &[String], piped: bool) -> Result<Child, Error> {
        let lease = self.subscribe(true).await?;
        let mut child = Command::new(binary("amcrust-output")).args(args).stdin(Stdio::piped())
            .stdout(if piped { Stdio::piped() } else { Stdio::null() }).stderr(Stdio::piped()).kill_on_drop(true).spawn()?;
        let input = child.stdin.take().ok_or("sink stdin")?;
        tokio::spawn(async move { if let Err(e) = feed(lease, input, false).await { log::debug!("native consumer retired: {e}"); } });
        Ok(child)
    }
    pub async fn audio(self: &Arc<Self>, input: ChildStdin) -> Result<(), Error> {
        let lease = self.subscribe(false).await?;
        tokio::spawn(async move { if let Err(e) = feed(lease, input, true).await { log::debug!("audio consumer retired: {e}"); } });
        Ok(())
    }
}

async fn feed(mut lease: Lease, mut out: ChildStdin, adts: bool) -> Result<(), Error> {
    let mut sequence = 0;
    let mut audio = None;
    loop {
        let message = timeout(Duration::from_secs(5), lease.rx.recv()).await?.ok_or("source closed")?;
        if !lease.alive.load(Ordering::SeqCst) || message.frame.born.elapsed() >= RESIDENCE { return Err("consumer retired or expired".into()); }
        let operation = async {
            if let Some(headers) = &message.headers {
                for header in headers {
                    if adts && header.metadata["codec"] == "aac" {
                        let asc = header.bytes("extradata")?;
                        if asc.len() < 2 || asc[0] >> 3 != 2 { return Err::<(),Error>("AAC-LC required".into()); }
                        let freq = ((asc[0]&7)<<1)|(asc[1]>>7);
                        let channels = (asc[1]>>3)&15;
                        if freq > 12 || !(1..=7).contains(&channels) { return Err("unsupported AAC configuration".into()); }
                        audio = Some((header.metadata["index"].as_u64().ok_or("audio index")?,freq,channels));
                    }
                    if !adts { header.write(&mut out, None).await?; }
                }
            }
            if adts {
                let (index, freq, channels) = audio.ok_or("missing AAC descriptor")?;
                if message.frame.metadata["stream"].as_u64() != Some(index) { return Ok(()); }
                let payload = message.frame.bytes("data")?;
                let len = payload.len()+7;
                if len > 8191 { return Err("AAC access unit too large".into()); }
                let header = [0xff,0xf1,0x40 | (freq<<2) | (channels>>2), ((channels&3)<<6)|(len>>11) as u8,
                    (len>>3) as u8, ((len&7)<<5) as u8|0x1f,0xfc];
                out.write_all(&header).await?; out.write_all(payload).await?;
            } else { message.frame.write(&mut out, Some(sequence)).await?; sequence += 1; }
            Ok(())
        };
        timeout(RESIDENCE.saturating_sub(message.frame.born.elapsed()), operation).await??;
    }
}
