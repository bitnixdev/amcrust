//! Encoder-keyed shared acquisition. This library is not yet wired to HomeKit.
//!
//! Registry owns async supervisors; each active encoder has exactly one blocking
//! Reader job. Retirement revokes deliveries before joining that job. Retry never
//! splices generations into an existing subscription: callers must resubscribe.
//! Call Registry::shutdown before dropping the runtime. Drop signals shutdown but
//! cannot synchronously join async cleanup; no task is aborted during native I/O.
mod admission;
pub mod clock;
mod subscription;
pub(crate) use admission::nals;
pub(crate) use subscription::DeliveryGuard;
pub use subscription::{Delivery, Subscription, SubscriptionLimits, Tracks};

use crate::ingest::{self, CameraUrl, Cancellation, Packet, Reader, StreamConfiguration};
use admission::Admission;
use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use subscription::Queue;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoder {
    Main,
    SubStream2,
}
/// Opaque camera identity assigned by configuration, never a credential-bearing URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourceKey {
    pub camera: u64,
    pub encoder: Encoder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Terminal {
    Cancelled,
    Disabled,
    Reconfigured,
    Shutdown,
    Source(ingest::Error),
    ConfigurationChanged,
    Clock,
    Unsupported,
    Budget,
    Residence,
    MediaSpan,
    AdmissionDeadline,
    RetryExhausted,
    WorkerFailed,
}
impl fmt::Display for Terminal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "shared source: {self:?}")
    }
}
impl std::error::Error for Terminal {}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub sources: usize,
    pub subscribers: usize,
    pub subscribers_per_source: usize,
    pub bytes: usize,
    pub bytes_per_source: usize,
    pub reader: ingest::Limits,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub failures_before_reset: u32,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            sources: 16,
            subscribers: 64,
            subscribers_per_source: 8,
            bytes: 64 * 1024 * 1024,
            bytes_per_source: 16 * 1024 * 1024,
            reader: ingest::Limits::default(),
            retry_initial: Duration::from_millis(250),
            retry_max: Duration::from_secs(8),
            failures_before_reset: 5,
        }
    }
}
impl Limits {
    fn valid(&self) -> bool {
        (1..=64).contains(&self.sources)
            && (1..=1024).contains(&self.subscribers)
            && (1..=64).contains(&self.subscribers_per_source)
            && self.bytes > 0
            && self.bytes <= u32::MAX as usize
            && self.bytes_per_source > 0
            && self.bytes_per_source <= self.bytes
            && self.reader.packet_bytes > 0
            && self.reader.packet_bytes <= self.bytes_per_source
            && !self.reader.startup.is_zero()
            && !self.reader.read.is_zero()
            && !self.retry_initial.is_zero()
            && self.retry_max >= self.retry_initial
            && self.retry_max <= Duration::from_secs(60)
            && self.failures_before_reset > 0
    }
}
struct Stop {
    stopped: AtomicBool,
    wake: Notify,
}
impl Stop {
    fn request(&self) {
        self.stopped.store(true, Ordering::Release);
        self.wake.notify_one();
    }
}
#[derive(Default)]
struct Metrics {
    active: AtomicBool,
    generation: AtomicU64,
    opens: AtomicU64,
    faults: AtomicU64,
    subscribers: AtomicUsize,
    missing_clocks: AtomicU64,
    oldest_ms: AtomicU64,
    clock: Mutex<Option<clock::Observations>>,
}
#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub active: bool,
    pub generation: u64,
    pub open_attempts: u64,
    pub faults: u64,
    pub subscribers: usize,
    pub skipped_initial_clocks: u64,
    pub charged_bytes: usize,
    /// Queued AND held delivery age for active subscriptions, not just channel residence.
    pub oldest_delivery_ms: u64,
}

/// Non-secret consumer handle. Credentials remain inside the supervisor.
#[derive(Clone)]
pub struct Source {
    tx: mpsc::Sender<Command>,
    stop: Arc<Stop>,
    metrics: Arc<Metrics>,
    bytes: Arc<Semaphore>,
    byte_limit: usize,
}
impl Source {
    pub async fn subscribe(
        &self,
        tracks: Tracks,
        limits: SubscriptionLimits,
    ) -> Result<Subscription, Terminal> {
        if self.stop.stopped.load(Ordering::Acquire) {
            return Err(Terminal::Shutdown);
        }
        if !limits.valid() {
            return Err(Terminal::Budget);
        }
        let (reply, answer) = oneshot::channel();
        self.tx
            .try_send(Command::Subscribe {
                tracks,
                limits,
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Terminal::Budget,
                mpsc::error::TrySendError::Closed(_) => Terminal::Shutdown,
            })?;
        answer.await.map_err(|_| Terminal::Shutdown)?
    }
    /// Existing subscriptions are retired, never silently re-enabled. This does
    /// not yet revoke copies in the legacy HomeKit transport/recording paths.
    pub async fn set_enabled(&self, enabled: bool) -> Result<(), Terminal> {
        let (reply, answer) = oneshot::channel();
        self.tx
            .send(Command::Enable(enabled, reply))
            .await
            .map_err(|_| Terminal::Shutdown)?;
        answer.await.map_err(|_| Terminal::Shutdown)
    }
    pub async fn reset(&self) -> Result<(), Terminal> {
        let (reply, answer) = oneshot::channel();
        self.tx
            .send(Command::Reset(None, reply))
            .await
            .map_err(|_| Terminal::Shutdown)?;
        answer.await.map_err(|_| Terminal::Shutdown)
    }
    /// Numeric, bounded metadata for the current/latest generation. None while
    /// opening; retired observations are never reused by a replacement reader.
    /// This is diagnostic evidence, not A/V clock or transport admission.
    pub fn clock_snapshot(&self) -> Option<clock::Snapshot> {
        self.metrics
            .clock
            .lock()
            .unwrap()
            .as_ref()
            .map(clock::Observations::snapshot)
    }
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            active: self.metrics.active.load(Ordering::Acquire),
            generation: self.metrics.generation.load(Ordering::Relaxed),
            open_attempts: self.metrics.opens.load(Ordering::Relaxed),
            faults: self.metrics.faults.load(Ordering::Relaxed),
            subscribers: self.metrics.subscribers.load(Ordering::Relaxed),
            skipped_initial_clocks: self.metrics.missing_clocks.load(Ordering::Relaxed),
            charged_bytes: self.byte_limit - self.bytes.available_permits(),
            oldest_delivery_ms: self.metrics.oldest_ms.load(Ordering::Relaxed),
        }
    }
}
struct Owner {
    source: Source,
    task: Option<JoinHandle<()>>,
}
pub struct Registry {
    owners: HashMap<SourceKey, Owner>,
    bytes: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    limits: Limits,
    closed: bool,
    join_failed: bool,
}
impl Registry {
    pub fn new(limits: Limits) -> Result<Self, Terminal> {
        if !limits.valid() {
            return Err(Terminal::Budget);
        }
        Ok(Self {
            owners: HashMap::new(),
            bytes: Arc::new(Semaphore::new(limits.bytes)),
            slots: Arc::new(Semaphore::new(limits.subscribers)),
            limits,
            closed: false,
            join_failed: false,
        })
    }
    /// Register once per camera/encoder. Duplicate keys are refused, not replaced
    /// while the previous reader might still be alive. Must run inside Tokio.
    pub fn register(&mut self, key: SourceKey, url: CameraUrl) -> Result<Source, Terminal> {
        if self.closed {
            return Err(Terminal::Shutdown);
        }
        if self.owners.contains_key(&key) {
            return Err(Terminal::Reconfigured);
        }
        if self.owners.len() >= self.limits.sources {
            return Err(Terminal::Budget);
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| Terminal::WorkerFailed)?;
        let (tx, rx) = mpsc::channel(16);
        let source = Source {
            tx,
            stop: Arc::new(Stop {
                stopped: AtomicBool::new(false),
                wake: Notify::new(),
            }),
            metrics: Arc::new(Metrics::default()),
            bytes: Arc::new(Semaphore::new(self.limits.bytes_per_source)),
            byte_limit: self.limits.bytes_per_source,
        };
        let supervisor = Supervisor {
            url,
            rx,
            stop: source.stop.clone(),
            metrics: source.metrics.clone(),
            source_bytes: source.bytes.clone(),
            aggregate_bytes: self.bytes.clone(),
            source_slots: Arc::new(Semaphore::new(self.limits.subscribers_per_source)),
            aggregate_slots: self.slots.clone(),
            limits: self.limits,
            clients: Vec::new(),
            running: None,
            enabled: true,
            generation: 0,
            failures: 0,
            next_open: Instant::now(),
        };
        let task = runtime.spawn(supervisor.run());
        self.owners.insert(
            key,
            Owner {
                source: source.clone(),
                task: Some(task),
            },
        );
        Ok(source)
    }
    pub fn source(&self, key: SourceKey) -> Option<Source> {
        self.owners.get(&key).map(|o| o.source.clone())
    }
    pub fn charged_bytes(&self) -> usize {
        self.limits.bytes - self.bytes.available_permits()
    }
    /// Owner-only URL replacement; no downstream consumer receives the URL.
    pub async fn configure(&self, key: SourceKey, url: CameraUrl) -> Result<(), Terminal> {
        let owner = self.owners.get(&key).ok_or(Terminal::Unsupported)?;
        let (reply, answer) = oneshot::channel();
        owner
            .source
            .tx
            .send(Command::Reset(Some(url), reply))
            .await
            .map_err(|_| Terminal::Shutdown)?;
        answer.await.map_err(|_| Terminal::Shutdown)
    }
    /// Signal every source before awaiting any join. Cancellation of this future
    /// leaves handles in Registry so shutdown may be awaited again.
    pub async fn shutdown(&mut self) -> Result<(), Terminal> {
        self.closed = true;
        for owner in self.owners.values() {
            owner.source.stop.request();
        }
        for owner in self.owners.values_mut() {
            if let Some(task) = owner.task.as_mut() {
                if task.await.is_err() {
                    self.join_failed = true;
                }
                owner.task = None;
            }
        }
        self.owners.clear();
        if self.join_failed {
            Err(Terminal::WorkerFailed)
        } else {
            Ok(())
        }
    }
}
impl Drop for Registry {
    fn drop(&mut self) {
        for owner in self.owners.values() {
            owner.source.stop.request();
        }
    }
}

enum Command {
    Subscribe {
        tracks: Tracks,
        limits: SubscriptionLimits,
        reply: oneshot::Sender<Result<Subscription, Terminal>>,
    },
    Enable(bool, oneshot::Sender<()>),
    Reset(Option<CameraUrl>, oneshot::Sender<()>),
}
struct Generation {
    streams: Vec<StreamConfiguration>,
    reason: Mutex<Option<Terminal>>,
    _charges: Vec<OwnedSemaphorePermit>,
}
impl Generation {
    fn check(&self) -> Result<(), Terminal> {
        match *self.reason.lock().unwrap() {
            Some(reason) => Err(reason),
            None => Ok(()),
        }
    }
    fn retire(&self, reason: Terminal) {
        self.reason.lock().unwrap().get_or_insert(reason);
    }
}
struct Client {
    queue: Weak<Queue>,
    started: bool,
}
struct Charged<T> {
    value: T,
    charges: Vec<OwnedSemaphorePermit>,
}
enum Event {
    Configuration(Charged<Vec<StreamConfiguration>>),
    Packet(Charged<Arc<Packet>>),
}
fn charge<T>(
    value: T,
    size: usize,
    source: &Arc<Semaphore>,
    aggregate: &Arc<Semaphore>,
) -> Result<Charged<T>, ingest::Error> {
    let size = u32::try_from(size).map_err(|_| ingest::Error::PacketLimit)?;
    let mut charges = Vec::new();
    for budget in [source, aggregate] {
        charges.push(
            budget
                .clone()
                .try_acquire_many_owned(size)
                .map_err(|_| ingest::Error::PacketLimit)?,
        );
    }
    Ok(Charged { value, charges })
}
struct Running {
    cancel: Cancellation,
    task: JoinHandle<Result<(), ingest::Error>>,
    events: mpsc::Receiver<Event>,
    configuration: Option<Arc<Generation>>,
    admission: Option<Admission>,
    born: Instant,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.events.close();
    }
}
struct Supervisor {
    url: CameraUrl,
    rx: mpsc::Receiver<Command>,
    stop: Arc<Stop>,
    metrics: Arc<Metrics>,
    source_bytes: Arc<Semaphore>,
    aggregate_bytes: Arc<Semaphore>,
    source_slots: Arc<Semaphore>,
    aggregate_slots: Arc<Semaphore>,
    limits: Limits,
    clients: Vec<Client>,
    running: Option<Running>,
    enabled: bool,
    generation: u64,
    failures: u32,
    next_open: Instant,
}
impl Drop for Supervisor {
    fn drop(&mut self) {
        // Also fence consumers if the async supervisor unwinds. Running's Drop
        // interrupts the reader and closes its handoff; normal paths join it.
        self.revoke(Terminal::WorkerFailed);
        if let Some(run) = &self.running {
            run.cancel.cancel();
        }
        self.metrics.active.store(false, Ordering::Release);
    }
}
impl Supervisor {
    async fn run(mut self) {
        let mut tick = tokio::time::interval(Duration::from_millis(25));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if self.stop.stopped.load(Ordering::Acquire) {
                break;
            }
            self.sweep();
            if self.clients.is_empty() && self.running.is_some() {
                self.retire(Terminal::Cancelled).await;
            }
            if self.running.is_none()
                && !self.clients.is_empty()
                && self.enabled
                && Instant::now() >= self.next_open
            {
                if self.failures >= self.limits.failures_before_reset {
                    self.retire(Terminal::RetryExhausted).await;
                } else if let Err(reason) = self.start() {
                    self.retire(reason).await;
                }
            }
            tokio::select! {
                _ = self.stop.wake.notified() => (),
                command = self.rx.recv() => match command {
                    Some(command) => self.command(command).await,
                    None => break,
                },
                _ = tick.tick() => (),
                event = async { self.running.as_mut().unwrap().events.recv().await }, if self.running.is_some() => {
                    match event {
                        Some(event) => if let Err(reason) = self.event(event) { self.fault(reason).await; },
                        None => {
                            let outcome = (&mut self.running.as_mut().unwrap().task).await;
                            let reason = match outcome {
                                Ok(Err(error)) => Terminal::Source(error),
                                _ => Terminal::WorkerFailed,
                            };
                            // Mark this handle consumed so retire does not poll it again.
                            self.fault_finished(reason).await;
                        }
                    }
                },
            }
        }
        self.retire(Terminal::Shutdown).await;
        self.rx.close();
        // Pending oneshots close with the supervisor, waking all callers.
    }
    fn sweep(&mut self) {
        let mut oldest = Duration::ZERO;
        self.clients.retain(|client| {
            let Some(queue) = client.queue.upgrade() else {
                return false;
            };
            queue.sweep(client.started);
            oldest = oldest.max(queue.oldest_age());
            queue.reason().is_none()
        });
        self.metrics
            .subscribers
            .store(self.clients.len(), Ordering::Relaxed);
        self.metrics.oldest_ms.store(
            oldest.as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
    async fn command(&mut self, command: Command) {
        match command {
            Command::Subscribe {
                tracks,
                limits,
                reply,
            } => {
                if reply.is_closed() {
                    return;
                }
                let result = if !self.enabled {
                    Err(Terminal::Disabled)
                } else if self.failures >= self.limits.failures_before_reset {
                    Err(Terminal::RetryExhausted)
                } else {
                    self.add(tracks, limits)
                };
                let _ = reply.send(result);
            }
            Command::Enable(enabled, reply) => {
                self.enabled = enabled;
                if !enabled {
                    self.retire(Terminal::Disabled).await;
                }
                let _ = reply.send(());
            }
            Command::Reset(url, reply) => {
                self.retire(Terminal::Reconfigured).await;
                if let Some(url) = url {
                    self.url = url;
                }
                self.failures = 0;
                self.next_open = Instant::now();
                let _ = reply.send(());
            }
        }
    }
    fn add(
        &mut self,
        tracks: Tracks,
        limits: SubscriptionLimits,
    ) -> Result<Subscription, Terminal> {
        if self
            .running
            .as_ref()
            .and_then(|run| run.configuration.as_ref())
            .is_some_and(|configuration| !tracks.supported(&configuration.streams))
        {
            return Err(Terminal::Unsupported);
        }
        let slots = vec![
            self.source_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Terminal::Budget)?,
            self.aggregate_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Terminal::Budget)?,
        ];
        let queue = Queue::new(tracks, limits, slots);
        self.clients.push(Client {
            queue: Arc::downgrade(&queue),
            started: false,
        });
        Ok(Subscription { queue })
    }
    fn start(&mut self) -> Result<(), Terminal> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(Terminal::RetryExhausted)?;
        let generation = self.generation;
        *self.metrics.clock.lock().unwrap() = None;
        let born = Instant::now();
        let url = self.url.clone();
        let limits = self.limits.reader;
        let cancel = Cancellation::default();
        let worker_cancel = cancel.clone();
        let source_bytes = self.source_bytes.clone();
        let aggregate_bytes = self.aggregate_bytes.clone();
        // Bounded handoff, not a per-consumer reader. At most eight packets are
        // transiently held here; reader.packet_bytes bounds each one separately.
        let (tx, events) = mpsc::channel(8);
        let task = tokio::task::spawn_blocking(move || {
            let mut reader = Reader::open(url, generation, worker_cancel, limits)?;
            let streams = reader.streams().to_vec();
            let size = streams
                .iter()
                .map(|s| {
                    s.parameters.owned_bytes()
                        + s.parameters.extradata().len()
                        + std::mem::size_of::<StreamConfiguration>()
                        + 512
                })
                .sum::<usize>();
            if size > 128 * 1024 {
                return Err(ingest::Error::PacketLimit);
            }
            let configuration = charge(streams, size, &source_bytes, &aggregate_bytes)?;
            tx.blocking_send(Event::Configuration(configuration))
                .map_err(|_| ingest::Error::Cancelled)?;
            loop {
                let packet = reader.read()?;
                let size = packet.owned_bytes();
                let packet = charge(packet, size, &source_bytes, &aggregate_bytes)?;
                tx.blocking_send(Event::Packet(packet))
                    .map_err(|_| ingest::Error::Cancelled)?;
            }
        });
        self.running = Some(Running {
            cancel,
            task,
            events,
            configuration: None,
            admission: None,
            born,
        });
        self.metrics.active.store(true, Ordering::Release);
        self.metrics.generation.store(generation, Ordering::Relaxed);
        self.metrics.opens.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    fn event(&mut self, event: Event) -> Result<(), Terminal> {
        let run = self.running.as_mut().unwrap();
        match event {
            Event::Configuration(configuration) => {
                let Charged {
                    value: streams,
                    charges,
                } = configuration;
                run.admission = Some(Admission::new(&streams)?);
                *self.metrics.clock.lock().unwrap() = Some(clock::Observations::new(
                    self.generation,
                    run.born,
                    &streams,
                ));
                for client in &self.clients {
                    if let Some(queue) = client.queue.upgrade()
                        && !queue.tracks.supported(&streams)
                    {
                        queue.retire(Terminal::Unsupported);
                    }
                }
                run.configuration = Some(Arc::new(Generation {
                    streams,
                    reason: Mutex::new(None),
                    _charges: charges,
                }));
            }
            Event::Packet(charged) => {
                let packet = charged.value;
                self.metrics
                    .clock
                    .lock()
                    .unwrap()
                    .as_mut()
                    .ok_or(Terminal::Unsupported)?
                    .observe(&packet)
                    .map_err(|_| Terminal::Clock)?;
                let Some(idr) = run
                    .admission
                    .as_mut()
                    .ok_or(Terminal::Unsupported)?
                    .inspect(&packet)?
                else {
                    self.metrics.missing_clocks.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                };
                let configuration = run.configuration.as_ref().ok_or(Terminal::Unsupported)?;
                let codec = configuration.streams[packet.stream_index()].codec;
                for client in &mut self.clients {
                    let Some(queue) = client.queue.upgrade() else {
                        continue;
                    };
                    if !queue.tracks.accepts(codec)
                        || (!client.started && queue.tracks.video() && !idr)
                    {
                        continue;
                    }
                    if let Err(reason) = queue.offer(
                        packet.clone(),
                        configuration.clone(),
                        &self.source_bytes,
                        &self.aggregate_bytes,
                    ) {
                        queue.retire(reason);
                    } else {
                        client.started = true;
                    }
                }
                // A successful sustained generation resets the circuit breaker;
                // merely opening a source that immediately fails does not.
                if run.born.elapsed() >= Duration::from_secs(30) {
                    self.failures = 0;
                }
            }
        }
        Ok(())
    }
    fn revoke(&mut self, reason: Terminal) {
        if let Some(clock) = self.metrics.clock.lock().unwrap().as_mut() {
            clock.retire();
        }
        if let Some(configuration) = self.running.as_ref().and_then(|r| r.configuration.as_ref()) {
            configuration.retire(reason);
        }
        for client in self.clients.drain(..) {
            if let Some(queue) = client.queue.upgrade() {
                queue.retire(reason);
            }
        }
        self.metrics.subscribers.store(0, Ordering::Relaxed);
        self.metrics.oldest_ms.store(0, Ordering::Relaxed);
    }
    async fn retire(&mut self, reason: Terminal) {
        self.revoke(reason);
        if let Some(mut run) = self.running.take() {
            run.cancel.cancel();
            // Closing the handoff unblocks blocking_send before waiting for join.
            run.events.close();
            while run.events.try_recv().is_ok() {}
            let _ = (&mut run.task).await;
        }
        self.metrics.active.store(false, Ordering::Release);
    }
    fn backoff(&mut self) {
        self.metrics.faults.fetch_add(1, Ordering::Relaxed);
        self.failures = self.failures.saturating_add(1);
        let delay = self
            .limits
            .retry_initial
            .saturating_mul(1u32 << self.failures.saturating_sub(1).min(16))
            .min(self.limits.retry_max);
        self.next_open = Instant::now() + delay;
    }
    async fn fault(&mut self, reason: Terminal) {
        self.retire(reason).await;
        self.backoff();
    }
    async fn fault_finished(&mut self, reason: Terminal) {
        self.revoke(reason);
        self.running.take(); // worker has already been joined
        self.metrics.active.store(false, Ordering::Release);
        self.backoff();
    }
}
