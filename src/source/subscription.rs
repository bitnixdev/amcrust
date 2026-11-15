use super::{Generation, Terminal};
use crate::ingest::{Packet, StreamConfiguration};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone, Copy)]
pub enum Tracks {
    Video,
    Audio,
    /// Starts at a future video IDR; does not provide covering AAC history for
    /// recording cuts. Recording history/clip admission is a separate owner.
    AudioVideo,
}
impl Tracks {
    pub(super) fn video(self) -> bool {
        matches!(self, Self::Video | Self::AudioVideo)
    }
    pub(super) fn supported(self, streams: &[StreamConfiguration]) -> bool {
        use crate::ingest::Codec;
        (!self.video() || streams.iter().any(|s| s.codec == Codec::H264))
            && (!matches!(self, Self::Audio | Self::AudioVideo)
                || streams.iter().any(|s| s.codec == Codec::Aac))
    }
    pub(super) fn accepts(self, codec: crate::ingest::Codec) -> bool {
        match codec {
            crate::ingest::Codec::H264 => self.video(),
            crate::ingest::Codec::Aac => matches!(self, Self::Audio | Self::AudioVideo),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SubscriptionLimits {
    /// Queued plus handed-out packets, not just channel capacity.
    pub packets: usize,
    pub bytes: usize,
    pub residence: Duration,
    /// Maximum PTS span within each track, without independently rebasing tracks.
    pub media_span: Duration,
    pub admission: Duration,
}
impl Default for SubscriptionLimits {
    fn default() -> Self {
        Self {
            packets: 128,
            bytes: 4 * 1024 * 1024,
            residence: Duration::from_secs(2),
            media_span: Duration::from_secs(2),
            admission: Duration::from_secs(15),
        }
    }
}
impl SubscriptionLimits {
    pub(super) fn valid(self) -> bool {
        self.packets > 0
            && self.packets <= 4096
            && self.bytes > 0
            && self.bytes <= u32::MAX as usize
            && !self.residence.is_zero()
            && !self.media_span.is_zero()
            && !self.admission.is_zero()
            && self.residence <= Duration::from_secs(60)
            && self.admission <= Duration::from_secs(60)
    }
}

pub(super) struct Validity {
    reason: Mutex<Option<Terminal>>,
    wake: Notify,
}
impl Validity {
    fn reason(&self) -> Option<Terminal> {
        *self.reason.lock().unwrap()
    }
    fn retire(&self, reason: Terminal) {
        self.reason.lock().unwrap().get_or_insert(reason);
        self.wake.notify_waiters();
    }
}
struct Entry {
    born: Instant,
    stream: usize,
    start: i128,
    end: i128,
}
struct Budget {
    bytes: Arc<Semaphore>,
    packets: Arc<Semaphore>,
    entries: Mutex<BTreeMap<u64, Entry>>,
    limits: SubscriptionLimits,
}

/// A non-cloneable delivery keeps all byte/count charges until dropped, even
/// after source retirement or removal from the queue. Access checks are cooperative:
/// already borrowed/copied bytes cannot be retracted and transport owners must
/// fence writes too. This is not yet the recording copied-output privacy boundary.
pub struct Delivery {
    packet: Arc<Packet>,
    generation: Arc<Generation>,
    validity: Arc<Validity>,
    budget: Arc<Budget>,
    _charges: Vec<OwnedSemaphorePermit>,
}
impl Drop for Delivery {
    fn drop(&mut self) {
        self.budget
            .entries
            .lock()
            .unwrap()
            .remove(&self.packet.sequence());
    }
}
impl Delivery {
    fn check(&self) -> Result<(), Terminal> {
        if self.packet.received_at().elapsed() >= self.budget.limits.residence {
            self.validity.retire(Terminal::Residence);
        }
        if let Some(reason) = self.validity.reason() {
            return Err(reason);
        }
        self.generation.check()
    }
    pub fn packet(&self) -> Result<&Packet, Terminal> {
        self.check()?;
        Ok(&self.packet)
    }
    pub fn configuration(&self) -> Result<&[StreamConfiguration], Terminal> {
        self.check()?;
        Ok(&self.generation.streams)
    }
}

pub(super) struct Queue {
    queue: Mutex<VecDeque<Delivery>>,
    validity: Arc<Validity>,
    budget: Arc<Budget>,
    pub(super) tracks: Tracks,
    pub(super) joined: Instant,
    pub(super) limits: SubscriptionLimits,
    // Retain admission slots even if a failed subscription is held by its caller.
    _slots: Vec<OwnedSemaphorePermit>,
}
impl Queue {
    pub(super) fn new(
        tracks: Tracks,
        limits: SubscriptionLimits,
        slots: Vec<OwnedSemaphorePermit>,
    ) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(VecDeque::new()),
            validity: Arc::new(Validity {
                reason: Mutex::new(None),
                wake: Notify::new(),
            }),
            budget: Arc::new(Budget {
                bytes: Arc::new(Semaphore::new(limits.bytes)),
                packets: Arc::new(Semaphore::new(limits.packets)),
                entries: Mutex::new(BTreeMap::new()),
                limits,
            }),
            tracks,
            joined: Instant::now(),
            limits,
            _slots: slots,
        })
    }
    pub(super) fn reason(&self) -> Option<Terminal> {
        self.validity.reason()
    }
    pub(super) fn retire(&self, reason: Terminal) {
        self.validity.retire(reason);
        self.queue.lock().unwrap().clear();
    }
    pub(super) fn oldest_age(&self) -> Duration {
        self.budget
            .entries
            .lock()
            .unwrap()
            .values()
            .map(|v| v.born.elapsed())
            .max()
            .unwrap_or_default()
    }
    pub(super) fn sweep(&self, started: bool) {
        if self.oldest_age() >= self.limits.residence {
            self.retire(Terminal::Residence);
        }
        if !started && self.joined.elapsed() >= self.limits.admission {
            self.retire(Terminal::AdmissionDeadline);
        }
        if let Some(reason) = self.reason() {
            self.retire(reason);
        }
    }
    pub(super) fn offer(
        &self,
        packet: Arc<Packet>,
        generation: Arc<Generation>,
        source: &Arc<Semaphore>,
        aggregate: &Arc<Semaphore>,
    ) -> Result<(), Terminal> {
        // Serialize insertion with subscription Drop/retirement. Never await while
        // holding this lock; publication cannot wait behind a slow consumer.
        let mut queue = self.queue.lock().unwrap();
        if let Some(reason) = self.reason() {
            return Err(reason);
        }
        if packet.received_at().elapsed() >= self.limits.residence {
            return Err(Terminal::Residence);
        }
        let stream = &generation.streams[packet.stream_index()];
        let start = packet.pts().ok_or(Terminal::Clock)? as i128;
        let end = start + packet.duration().max(0) as i128;
        {
            let entries = self.budget.entries.lock().unwrap();
            let (mut low, mut high) = (start, end);
            for entry in entries
                .values()
                .filter(|v| v.stream == packet.stream_index())
            {
                low = low.min(entry.start);
                high = high.max(entry.end);
            }
            let span_ns = (high - low) * stream.time_base.numerator as i128 * 1_000_000_000;
            if span_ns
                > self.limits.media_span.as_nanos().min(i128::MAX as u128) as i128
                    * stream.time_base.denominator as i128
            {
                return Err(Terminal::MediaSpan);
            }
        }
        let size = packet
            .owned_bytes()
            .checked_add(std::mem::size_of::<Delivery>() + std::mem::size_of::<Entry>() + 128)
            .and_then(|n| u32::try_from(n).ok())
            .ok_or(Terminal::Budget)?;
        let mut charges = Vec::with_capacity(4);
        charges.push(
            self.budget
                .packets
                .clone()
                .try_acquire_owned()
                .map_err(|_| Terminal::Budget)?,
        );
        for bytes in [&self.budget.bytes, source, aggregate] {
            charges.push(
                bytes
                    .clone()
                    .try_acquire_many_owned(size)
                    .map_err(|_| Terminal::Budget)?,
            );
        }
        self.budget.entries.lock().unwrap().insert(
            packet.sequence(),
            Entry {
                born: packet.received_at(),
                stream: packet.stream_index(),
                start,
                end,
            },
        );
        queue.push_back(Delivery {
            packet,
            generation,
            validity: self.validity.clone(),
            budget: self.budget.clone(),
            _charges: charges,
        });
        drop(queue);
        self.validity.wake.notify_one();
        Ok(())
    }
}

pub struct Subscription {
    pub(super) queue: Arc<Queue>,
}
impl Drop for Subscription {
    fn drop(&mut self) {
        self.queue.retire(Terminal::Cancelled);
    }
}
impl Subscription {
    /// Terminal notification wins over buffered packets. Cancellation of recv does
    /// not remove a packet or release its charge before it has been returned.
    pub async fn recv(&mut self) -> Result<Delivery, Terminal> {
        loop {
            let notified = self.queue.validity.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(reason) = self.queue.reason() {
                return Err(reason);
            }
            let delivery = self.queue.queue.lock().unwrap().pop_front();
            if let Some(delivery) = delivery {
                delivery.check()?;
                return Ok(delivery);
            }
            notified.await;
        }
    }
    pub fn terminal(&self) -> Option<Terminal> {
        self.queue.reason()
    }
    pub fn charged_bytes(&self) -> usize {
        self.queue.limits.bytes - self.queue.budget.bytes.available_permits()
    }
    pub fn outstanding_packets(&self) -> usize {
        self.queue.limits.packets - self.queue.budget.packets.available_permits()
    }
}
