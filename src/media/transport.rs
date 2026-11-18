//! Per-session secure UDP boundary shared by production proxies and Rust producers.
//! No camera acquisition, timestamp rewriting, pacing or implicit RTCP generation.
//! Session revoke is a synchronous socket-write fence, not a retraction of datagrams
//! already accepted by the kernel. Source-bound transports additionally serialize
//! kernel writes with generation/subscription retirement; unbound adapters must
//! still have their owner revoke before acknowledging source privacy transitions.
use super::rtcp::{Counts, payload_octets};
use crate::source::{Delivery, DeliveryGuard};
use crate::srtp::SrtpSession;
use std::{net::SocketAddr, sync::Mutex, time::Duration};
use tokio::{
    net::UdpSocket,
    sync::{Mutex as AsyncMutex, watch},
};

pub const RTP_OVERHEAD: usize = 10;
pub const RTCP_OVERHEAD: usize = 14;
const MAX_DATAGRAM: usize = 2048;
const WRITE_DEADLINE: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub enum Error {
    Closed,
    Busy,
    Revoked,
    TooLarge,
    Packet,
    Authentication,
    Deadline,
    Io(std::io::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "media transport: {self:?}")
    }
}
impl std::error::Error for Error {}

struct State {
    closed: bool,
    crypto: SrtpSession,
    counts: Counts,
}
pub struct Transport {
    socket: UdpSocket,
    peer: SocketAddr,
    ssrc: u32,
    wire_mtu: usize,
    state: Mutex<State>,
    closed: watch::Sender<bool>,
    send_slot: AsyncMutex<()>,
    receive_slot: AsyncMutex<()>,
    source: Option<DeliveryGuard>,
}
impl Transport {
    /// `wire_mtu` bounds outbound protected datagrams. Incoming feedback has an
    /// independent 2048-byte cap. The socket must be unconnected and exclusively
    /// owned here; each viewer/track gets its own transport and key context.
    pub fn new(
        socket: UdpSocket,
        peer: SocketAddr,
        key: &[u8],
        ssrc: u32,
        wire_mtu: usize,
    ) -> Option<Self> {
        if !(42..=MAX_DATAGRAM).contains(&wire_mtu)
            || peer.port() == 0
            || socket.peer_addr().is_ok()
        {
            return None;
        }
        Some(Self {
            socket,
            peer,
            ssrc,
            wire_mtu,
            state: Mutex::new(State {
                closed: false,
                crypto: SrtpSession::new(key)?,
                counts: Counts::default(),
            }),
            closed: watch::channel(false).0,
            send_slot: AsyncMutex::new(()),
            receive_slot: AsyncMutex::new(()),
            source: None,
        })
    }
    /// Bind permanently to the actual source generation AND subscription of this
    /// delivery. A new generation requires a new transport; no rebinding API exists.
    /// Retirement serializes with kernel writes, including already copied payloads.
    pub fn new_source_bound(
        socket: UdpSocket,
        peer: SocketAddr,
        key: &[u8],
        ssrc: u32,
        wire_mtu: usize,
        first: &Delivery,
    ) -> Result<Self, Error> {
        Self::from_guard(
            socket,
            peer,
            key,
            ssrc,
            wire_mtu,
            first.guard().map_err(|_| Error::Revoked)?,
        )
    }
    /// Fence viewer writes with the subscription that produced this Opus stream.
    pub fn new_opus_bound(
        socket: UdpSocket,
        peer: SocketAddr,
        key: &[u8],
        ssrc: u32,
        wire_mtu: usize,
        packet: &super::audio::OpusPacket,
    ) -> Result<Self, Error> {
        Self::from_guard(socket, peer, key, ssrc, wire_mtu, packet.source_guard())
    }
    pub(crate) fn from_guard(
        socket: UdpSocket,
        peer: SocketAddr,
        key: &[u8],
        ssrc: u32,
        wire_mtu: usize,
        source: DeliveryGuard,
    ) -> Result<Self, Error> {
        source.check().map_err(|_| Error::Revoked)?;
        let mut transport = Self::new(socket, peer, key, ssrc, wire_mtu).ok_or(Error::Packet)?;
        transport.source = Some(source);
        Ok(transport)
    }
    async fn source_retired(&self) {
        match &self.source {
            Some(source) => source.retired().await,
            None => std::future::pending::<()>().await,
        }
    }
    /// Whether transport revocation has been signaled; not a task-join acknowledgement.
    pub fn is_closed(&self) -> bool {
        *self.closed.borrow()
            || self
                .source
                .as_ref()
                .is_some_and(|source| source.check().is_err())
    }
    /// Snapshot of kernel-accepted RTP, retained after revocation. Not receiver
    /// acknowledgments. A session actor must serialize snapshot/report generation
    /// with its sends to associate the counters with the supplied clock pair.
    pub fn rtp_counts(&self) -> Counts {
        self.lock_state().counts
    }
    pub fn rtp_capacity(&self) -> usize {
        self.wire_mtu - RTP_OVERHEAD
    }
    /// After return no subsequent socket write can pass this session's gate.
    /// Pending readiness/receive waits are woken; owners must still join their tasks.
    pub fn revoke(&self) {
        self.lock_state().closed = true;
        self.closed.send_replace(true);
    }
    fn lock_state(&self) -> std::sync::MutexGuard<'_, State> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                // A panicking producer check must not make revoke/Drop panic or
                // allow reuse of potentially interrupted cryptographic state.
                let mut state = poisoned.into_inner();
                state.closed = true;
                self.closed.send_replace(true);
                state
            }
        }
    }
    /// `current` is checked at sealing and again immediately before the nonblocking
    /// write. It must be fast, nonblocking and must not call this transport. It is
    /// NOT an atomic source-generation fence by itself: use `new_source_bound` for
    /// source consumers, or have an unbound adapter's owner explicitly revoke.
    pub async fn send_rtp(&self, packet: &[u8], current: impl Fn() -> bool) -> Result<(), Error> {
        self.send(packet, false, current).await
    }
    pub async fn send_rtcp(&self, packet: &[u8], current: impl Fn() -> bool) -> Result<(), Error> {
        self.send(packet, true, current).await
    }
    async fn send(
        &self,
        packet: &[u8],
        rtcp: bool,
        current: impl Fn() -> bool,
    ) -> Result<(), Error> {
        let mut closed = self.closed.subscribe();
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let _slot = self.send_slot.try_lock().map_err(|_| Error::Busy)?;
        let overhead = if rtcp { RTCP_OVERHEAD } else { RTP_OVERHEAD };
        if packet.len() > self.wire_mtu - overhead {
            return Err(Error::TooLarge);
        }
        let offset = if rtcp { 4 } else { 8 };
        if packet.len() < offset + 4
            || packet[0] >> 6 != 2
            || packet[offset..offset + 4] != self.ssrc.to_be_bytes()
        {
            return Err(Error::Packet);
        }
        let octets = if rtcp {
            None
        } else {
            Some(payload_octets(packet).map_err(|_| Error::Packet)?)
        };
        let protected = {
            let mut state = self.lock_state();
            if state.closed {
                return Err(Error::Closed);
            }
            if !current() {
                return Err(Error::Revoked);
            }
            if rtcp {
                state.crypto.protect_rtcp(packet)
            } else {
                state.crypto.protect_rtp(packet)
            }
            .ok_or(Error::Packet)?
        };
        if protected.len() > self.wire_mtu {
            return Err(Error::TooLarge);
        }
        let deadline = tokio::time::Instant::now() + WRITE_DEADLINE;
        loop {
            tokio::select! {
                biased;
                _ = closed.changed() => return Err(Error::Closed),
                _ = self.source_retired() => { self.revoke(); return Err(Error::Closed); },
                _ = tokio::time::sleep_until(deadline) => return Err(Error::Deadline),
                result = self.socket.writable() => result.map_err(Error::Io)?,
            }
            // No await while holding the gate. Reuse the SAME ciphertext on
            // WouldBlock: re-encrypting would advance SRTCP/rollover state twice.
            let mut state = self.lock_state();
            if state.closed {
                return Err(Error::Closed);
            }
            if !current() {
                return Err(Error::Revoked);
            }
            // Producer code runs ABOVE these source read locks. Only the nonblocking
            // syscall is inside them; retirement takes their write side before ack.
            let write = || self.socket.try_send_to(&protected, self.peer);
            let result = match &self.source {
                Some(source) => source.with_current(write),
                None => Ok(write()),
            };
            let result = match result {
                Ok(result) => result,
                Err(_) => {
                    state.closed = true;
                    self.closed.send_replace(true);
                    return Err(Error::Closed);
                }
            };
            match result {
                Ok(n) if n == protected.len() => {
                    if let Some(octets) = octets {
                        state.counts.accepted(octets);
                    }
                    return Ok(());
                }
                Ok(_) => return Err(Error::Packet),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                Err(e) => return Err(Error::Io(e)),
            }
        }
    }
    /// At most one bounded receive; filters the negotiated peer and authenticates
    /// feedback with replay protection. A malformed datagram is a local refusal.
    pub async fn feedback(&self) -> Result<Vec<u8>, Error> {
        let mut closed = self.closed.subscribe();
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let _slot = self.receive_slot.try_lock().map_err(|_| Error::Busy)?;
        let mut bytes = [0u8; MAX_DATAGRAM + 1];
        let (n, peer) = tokio::select! {
            biased;
            _ = closed.changed() => return Err(Error::Closed),
            _ = self.source_retired() => { self.revoke(); return Err(Error::Closed); },
            result = self.socket.recv_from(&mut bytes) => result.map_err(Error::Io)?,
        };
        if peer != self.peer {
            return Err(Error::Authentication);
        }
        if n > MAX_DATAGRAM {
            return Err(Error::TooLarge);
        }
        let mut state = self.lock_state();
        if state.closed {
            return Err(Error::Closed);
        }
        state
            .crypto
            .unprotect_rtcp(&bytes[..n])
            .ok_or(Error::Authentication)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use webrtc_srtp::{context::Context, protection_profile::ProtectionProfile};

    async fn pair() -> (Arc<Transport>, UdpSocket) {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        (
            Arc::new(Transport::new(socket, peer.local_addr().unwrap(), &[7; 30], 42, 96).unwrap()),
            peer,
        )
    }
    fn rtp(sequence: u16, size: usize) -> Vec<u8> {
        let mut bytes = vec![0x55; size];
        bytes[..12].copy_from_slice(&[0x80, 110, 0, 0, 0, 1, 0x77, 0, 0, 0, 0, 42]);
        bytes[2..4].copy_from_slice(&sequence.to_be_bytes());
        bytes
    }
    fn report(ssrc: u32) -> Vec<u8> {
        let mut bytes = vec![0x80, 200, 0, 6];
        bytes.extend_from_slice(&ssrc.to_be_bytes());
        bytes.extend_from_slice(&[0; 20]);
        bytes
    }
    async fn receive(socket: &UdpSocket) -> Vec<u8> {
        let mut bytes = [0; 2048];
        let n = tokio::time::timeout(Duration::from_secs(1), socket.recv(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        bytes[..n].to_vec()
    }
    async fn no_packet(socket: &UdpSocket) {
        let mut bytes = [0; 2048];
        assert!(
            tokio::time::timeout(Duration::from_millis(20), socket.recv(&mut bytes))
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn exact_wire_budget_rollover_and_unchanged_clocks() {
        let (sender, peer) = pair().await;
        let mut decoder = Context::new(
            &[7; 16],
            &[7; 14],
            ProtectionProfile::Aes128CmHmacSha1_80,
            None,
            None,
        )
        .unwrap();
        assert_eq!(sender.rtp_capacity(), 86);
        for sequence in [u16::MAX, 0, 1] {
            let plain = rtp(sequence, 86);
            sender.send_rtp(&plain, || true).await.unwrap();
            let encrypted = receive(&peer).await;
            assert_eq!(encrypted.len(), 96);
            assert_eq!(decoder.decrypt_rtp(&encrypted).unwrap().as_ref(), plain);
        }
        assert_eq!(
            sender.rtp_counts(),
            Counts {
                packets: 3,
                octets: 222
            }
        );
        let plain = report(42);
        sender.send_rtcp(&plain, || true).await.unwrap();
        let encrypted = receive(&peer).await;
        assert_eq!(encrypted.len(), plain.len() + RTCP_OVERHEAD);
        assert_eq!(decoder.decrypt_rtcp(&encrypted).unwrap().as_ref(), plain);
        assert!(matches!(
            sender.send_rtp(&rtp(2, 87), || true).await,
            Err(Error::TooLarge)
        ));
        assert!(matches!(
            sender.send_rtcp(&report(43), || true).await,
            Err(Error::Packet)
        ));
        no_packet(&peer).await;
        assert_eq!(
            sender.rtp_counts(),
            Counts {
                packets: 3,
                octets: 222
            }
        );
    }
    #[tokio::test]
    async fn feedback_authentication_peer_binding_replay_and_ssrc_bounds() {
        let (sender, peer) = pair().await;
        let destination = sender.socket.local_addr().unwrap();
        let mut controller = SrtpSession::new(&[7; 30]).unwrap();
        let plain = report(99);
        let encrypted = controller.protect_rtcp(&plain).unwrap();
        let stranger = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        stranger.send_to(&encrypted, destination).await.unwrap();
        assert!(matches!(
            sender.feedback().await,
            Err(Error::Authentication)
        ));
        let mut corrupt = encrypted.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        peer.send_to(&corrupt, destination).await.unwrap();
        assert!(matches!(
            sender.feedback().await,
            Err(Error::Authentication)
        ));
        peer.send_to(&encrypted, destination).await.unwrap();
        assert_eq!(sender.feedback().await.unwrap(), plain);
        peer.send_to(&encrypted, destination).await.unwrap();
        assert!(matches!(
            sender.feedback().await,
            Err(Error::Authentication)
        ));
        let changed = controller.protect_rtcp(&report(100)).unwrap();
        peer.send_to(&changed, destination).await.unwrap();
        assert!(matches!(
            sender.feedback().await,
            Err(Error::Authentication)
        ));
        let next = controller.protect_rtcp(&plain).unwrap();
        peer.send_to(&next, destination).await.unwrap();
        assert_eq!(sender.feedback().await.unwrap(), plain);
        peer.send_to(&[0; MAX_DATAGRAM + 100], destination)
            .await
            .unwrap();
        assert!(matches!(sender.feedback().await, Err(Error::TooLarge)));
    }
    #[tokio::test]
    async fn producer_rechecked_at_write_and_close_wakes_receive() {
        let (sender, peer) = pair().await;
        let calls = AtomicUsize::new(0);
        assert!(matches!(
            sender
                .send_rtp(&rtp(1, 20), || calls.fetch_add(1, Ordering::SeqCst) == 0)
                .await,
            Err(Error::Revoked)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(sender.rtp_counts(), Counts::default());
        no_packet(&peer).await;
        let waiting = sender.clone();
        let task = tokio::spawn(async move { waiting.feedback().await });
        tokio::task::yield_now().await;
        sender.revoke();
        sender.revoke();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap(),
            Err(Error::Closed)
        ));
        assert!(matches!(
            sender.send_rtp(&rtp(2, 20), || true).await,
            Err(Error::Closed)
        ));
        assert!(matches!(
            sender.send_rtcp(&report(42), || true).await,
            Err(Error::Closed)
        ));
        no_packet(&peer).await;
        let (other, other_peer) = pair().await;
        other.send_rtp(&rtp(1, 20), || true).await.unwrap();
        assert!(!receive(&other_peer).await.is_empty());
    }
    #[tokio::test]
    async fn producer_panic_does_not_break_the_stop_fence() {
        let (sender, peer) = pair().await;
        let worker = sender.clone();
        let task = tokio::spawn(async move {
            worker
                .send_rtp(&rtp(1, 20), || panic!("producer failed"))
                .await
        });
        assert!(task.await.unwrap_err().is_panic());
        sender.revoke();
        assert!(matches!(
            sender.send_rtp(&rtp(2, 20), || true).await,
            Err(Error::Closed)
        ));
        no_packet(&peer).await;
    }

    #[tokio::test]
    async fn concurrent_calls_refused_without_an_internal_queue() {
        let (sender, peer) = pair().await;
        let _send = sender.send_slot.lock().await;
        let _receive = sender.receive_slot.lock().await;
        assert!(matches!(
            sender.send_rtp(&rtp(1, 20), || true).await,
            Err(Error::Busy)
        ));
        assert!(matches!(sender.feedback().await, Err(Error::Busy)));
        no_packet(&peer).await;
    }
}
