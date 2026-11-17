use super::{Generation, Retirement, Terminal};
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
    /// Audio-only opt-in for a bounded source-anchored decoded-sample clock.
    /// Allows proven bounded SR remapping, preserving original PTS/DTS. Consumers
    /// must still enforce cumulative phase/freshness budgets; not for muxing.
    AudioSampleClock,
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
            && (!matches!(
                self,
                Self::Audio | Self::AudioSampleClock | Self::AudioVideo
            ) || streams.iter().any(|s| s.codec == Codec::Aac))
    }
    pub(super) fn accepts_mapping_step(self) -> bool {
        matches!(self, Self::AudioSampleClock)
    }
    pub(super) fn accepts(self, codec: crate::ingest::Codec) -> bool {
        match codec {
            crate::ingest::Codec::H264 => self.video(),
            crate::ingest::Codec::Aac => matches!(
                self,
                Self::Audio | Self::AudioSampleClock | Self::AudioVideo
            ),
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
    reason: Retirement,
    wake: Notify,
}
impl Validity {
    fn reason(&self) -> Option<Terminal> {
        self.reason.reason()
    }
    fn retire(&self, reason: Terminal) {
        self.reason.retire(reason);
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
/// Internal output ownership token. It does not keep the first packet alive and
/// identifies the actual subscription/generation allocations, not just a numeric
/// generation (which can be equal for different encoders).
#[derive(Clone)]
pub(crate) struct DeliveryGuard {
    generation: Arc<Generation>,
    validity: Arc<Validity>,
}
impl DeliveryGuard {
    /// Hold only across a nonblocking syscall. Never await, call a source method,
    /// or invoke arbitrary producer code while these read locks are held.
    pub(crate) fn with_current<T>(&self, write: impl FnOnce() -> T) -> Result<T, Terminal> {
        let generation = self.generation.reason.0.borrow();
        let subscription = self.validity.reason.0.borrow();
        if let Some(reason) = *subscription {
            return Err(reason);
        }
        if let Some(reason) = *generation {
            return Err(reason);
        }
        Ok(write())
    }
    pub(crate) async fn retired(&self) {
        let mut generation = self.generation.reason.0.subscribe();
        let mut subscription = self.validity.reason.0.subscribe();
        if self.check().is_err() {
            return;
        }
        tokio::select! {
            _ = generation.changed() => (),
            _ = subscription.changed() => (),
        }
    }
    pub(crate) fn check(&self) -> Result<(), Terminal> {
        if let Some(reason) = self.validity.reason() {
            return Err(reason);
        }
        self.generation.check()
    }
    pub(crate) fn accepts(&self, delivery: &Delivery) -> Result<(), Terminal> {
        self.check()?;
        delivery.check()?;
        if !Arc::ptr_eq(&self.generation, &delivery.generation)
            || !Arc::ptr_eq(&self.validity, &delivery.validity)
        {
            return Err(Terminal::Reconfigured);
        }
        Ok(())
    }
}
impl Delivery {
    pub fn timing(&self) -> Result<super::Timing, Terminal> {
        self.check()?;
        Ok(self.generation.timing)
    }
    pub(crate) fn guard(&self) -> Result<DeliveryGuard, Terminal> {
        self.check()?;
        Ok(DeliveryGuard {
            generation: self.generation.clone(),
            validity: self.validity.clone(),
        })
    }
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
                reason: Retirement::default(),
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
    pub(super) fn quarantine_audio(&self) {
        if self.tracks.accepts(crate::ingest::Codec::Aac) {
            self.retire(Terminal::Clock);
        }
    }
    pub(super) fn admit_mapping_step(&self, bounded_mapping_step: bool) -> bool {
        if bounded_mapping_step
            && self.tracks.accepts(crate::ingest::Codec::Aac)
            && !self.tracks.accepts_mapping_step()
        {
            self.retire(Terminal::Clock);
            false
        } else {
            true
        }
    }
    pub(super) fn current_start(&self, packet: &Packet) -> bool {
        packet.received_at() >= self.joined
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
        let presentation = generation.timing == super::Timing::Presentation;
        let (start, end, numerator, denominator) = if presentation {
            let start = packet
                .received_at()
                .checked_duration_since(generation.born)
                .ok_or(Terminal::Clock)?
                .as_nanos() as i128;
            // This mode bounds read-time span, not an untrusted camera duration.
            // Count/byte/residence limits independently bound single deliveries.
            (start, start, 1, 1_000_000_000)
        } else {
            let start = packet.pts().ok_or(Terminal::Clock)? as i128;
            (
                start,
                start + packet.duration().max(0) as i128,
                stream.time_base.numerator,
                stream.time_base.denominator,
            )
        };
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
            let span_ns = (high - low) * numerator as i128 * 1_000_000_000;
            if span_ns
                > self.limits.media_span.as_nanos().min(i128::MAX as u128) as i128
                    * denominator as i128
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

#[cfg(test)]
mod tests {
    use super::*;
    fn generation() -> Arc<Generation> {
        Arc::new(Generation {
            timing: super::super::Timing::Source,
            born: Instant::now(),
            streams: vec![],
            reason: Retirement::default(),
            _charges: vec![],
        })
    }
    fn lease(generation: &Arc<Generation>, queue: &Arc<Queue>) -> DeliveryGuard {
        DeliveryGuard {
            generation: generation.clone(),
            validity: queue.validity.clone(),
        }
    }
    async fn transport(
        guard: DeliveryGuard,
    ) -> (
        Arc<crate::media::transport::Transport>,
        tokio::net::UdpSocket,
    ) {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender = crate::media::transport::Transport::from_guard(
            socket,
            peer.local_addr().unwrap(),
            &[7; 30],
            42,
            1200,
            guard,
        )
        .unwrap();
        (Arc::new(sender), peer)
    }
    const RTP: [u8; 13] = [0x80, 99, 0, 1, 0, 0, 0, 1, 0, 0, 0, 42, 0x65];
    #[tokio::test]
    async fn source_fence_wins_after_the_final_producer_predicate() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let generation = generation();
        let queue = Queue::new(Tracks::Video, SubscriptionLimits::default(), vec![]);
        let (sender, peer) = transport(lease(&generation, &queue)).await;
        let calls = AtomicUsize::new(0);
        let result = sender
            .send_rtp(&RTP, || {
                if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    queue.retire(Terminal::Cancelled);
                }
                true // Simulate stale/copy-only producer state, even at the final check.
            })
            .await;
        assert!(matches!(
            result,
            Err(crate::media::transport::Error::Closed)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(sender.rtp_counts().packets, 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), peer.recv(&mut [0; 2048]))
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn local_audio_retirement_preserves_video_but_generation_closes_both() {
        use crate::media::transport::{Error, Transport};
        let generation = generation();
        let audio = Queue::new(
            Tracks::AudioSampleClock,
            SubscriptionLimits::default(),
            vec![],
        );
        let video = Queue::new(Tracks::Video, SubscriptionLimits::default(), vec![]);
        let audio_guard = lease(&generation, &audio);
        let (a, _a_peer) = transport(audio_guard.clone()).await;
        let (v, v_peer) = transport(lease(&generation, &video)).await;
        let waiting = a.clone();
        let feedback = tokio::spawn(async move { waiting.feedback().await });
        audio.quarantine_audio();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), feedback)
                .await
                .unwrap()
                .unwrap(),
            Err(Error::Closed)
        ));
        assert!(matches!(
            a.send_rtp(&RTP, || true).await,
            Err(Error::Closed)
        ));
        v.send_rtp(&RTP, || true).await.unwrap();
        v_peer.recv(&mut [0; 2048]).await.unwrap();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert!(matches!(
            Transport::from_guard(
                socket,
                v_peer.local_addr().unwrap(),
                &[7; 30],
                42,
                1200,
                audio_guard
            ),
            Err(Error::Revoked)
        ));
        let waiting = v.clone();
        let feedback = tokio::spawn(async move { waiting.feedback().await });
        generation.retire(Terminal::Disabled);
        generation.retire(Terminal::Shutdown);
        assert_eq!(generation.check(), Err(Terminal::Disabled));
        assert!(v.is_closed());
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), feedback)
                .await
                .unwrap()
                .unwrap(),
            Err(Error::Closed)
        ));
        assert!(matches!(
            v.send_rtp(&RTP, || true).await,
            Err(Error::Closed)
        ));
        assert!(matches!(
            v.send_rtcp(&crate::media::rtcp::Sender::new(42, [0; 16]).bye(), || true)
                .await,
            Err(Error::Closed)
        ));
        assert_eq!(v.rtp_counts().packets, 1);
    }
    #[test]
    fn retirement_waits_for_an_entered_write_fence() {
        for local in [false, true] {
            let generation = generation();
            let queue = Queue::new(Tracks::Video, SubscriptionLimits::default(), vec![]);
            let guard = lease(&generation, &queue);
            let (entered, inside) = std::sync::mpsc::channel();
            let (release, released) = std::sync::mpsc::channel();
            let writer = std::thread::spawn(move || {
                guard.with_current(|| {
                    entered.send(()).unwrap();
                    released.recv().unwrap(); // Deliberately pause the simulated syscall.
                })
            });
            inside.recv().unwrap();
            let (started, starting) = std::sync::mpsc::channel();
            let (ack, acknowledged) = std::sync::mpsc::channel();
            let retiring = generation.clone();
            let retiring_queue = queue.clone();
            let owner = std::thread::spawn(move || {
                started.send(()).unwrap();
                if local {
                    retiring_queue.retire(Terminal::Reconfigured);
                } else {
                    retiring.retire(Terminal::Reconfigured);
                }
                ack.send(()).unwrap();
            });
            starting.recv().unwrap();
            let premature_ack = acknowledged.recv_timeout(Duration::from_millis(50)).is_ok();
            release.send(()).unwrap();
            writer.join().unwrap().unwrap();
            owner.join().unwrap();
            assert!(
                !premature_ack,
                "retirement acknowledged before the write completed"
            );
            acknowledged.recv().unwrap();
            assert_eq!(
                lease(&generation, &queue).with_current(|| panic!("retired write")),
                Err(Terminal::Reconfigured)
            );
        }
    }

    #[test]
    fn oversized_mapping_revokes_all_audio_but_not_shared_video() {
        let generation = Arc::new(Generation {
            timing: super::super::Timing::Source,
            born: Instant::now(),
            streams: vec![],
            reason: Retirement::default(),
            _charges: vec![],
        });
        for tracks in [
            Tracks::Audio,
            Tracks::AudioVideo,
            Tracks::AudioSampleClock,
            Tracks::Video,
        ] {
            let queue = Queue::new(tracks, SubscriptionLimits::default(), vec![]);
            let held = DeliveryGuard {
                generation: generation.clone(),
                validity: queue.validity.clone(),
            };
            queue.quarantine_audio();
            if tracks.accepts(crate::ingest::Codec::Aac) {
                assert_eq!(held.check(), Err(Terminal::Clock));
                queue.retire(Terminal::Shutdown);
                assert_eq!(held.check(), Err(Terminal::Clock));
            } else {
                assert_eq!(held.check(), Ok(()));
            }
            assert_eq!(generation.check(), Ok(()));
        }
    }

    #[test]
    fn remapping_retires_strict_consumers_without_revoking_other_tracks() {
        let generation = Arc::new(Generation {
            timing: super::super::Timing::Source,
            born: Instant::now(),
            streams: vec![],
            reason: Retirement::default(),
            _charges: vec![],
        });
        for tracks in [
            Tracks::Audio,
            Tracks::AudioVideo,
            Tracks::AudioSampleClock,
            Tracks::Video,
        ] {
            let queue = Queue::new(tracks, SubscriptionLimits::default(), vec![]);
            let held = DeliveryGuard {
                generation: generation.clone(),
                validity: queue.validity.clone(),
            };
            assert!(queue.admit_mapping_step(false));
            queue.admit_mapping_step(true);
            if matches!(tracks, Tracks::Audio | Tracks::AudioVideo) {
                assert_eq!(held.check(), Err(Terminal::Clock));
                queue.retire(Terminal::Shutdown);
                assert_eq!(held.check(), Err(Terminal::Clock));
            } else {
                assert_eq!(held.check(), Ok(()));
            }
            assert_eq!(generation.check(), Ok(()));
        }
    }
}
