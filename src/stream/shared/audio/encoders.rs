//! A manager owns one source. Each compatible group acquires one subscription;
//! neither a worker nor its history ever changes generation under a viewer.
use super::*;
use amcrust::media::audio::OpusPacket;
use std::{
    collections::{HashMap, VecDeque},
    sync::Weak,
};
use tokio::sync::broadcast;

type Packet = Arc<OpusPacket>;
struct Fanout<T: Clone> {
    live: broadcast::Sender<T>,
    history: VecDeque<T>,
    preroll: usize,
}
impl<T: Clone> Fanout<T> {
    fn new(frame_ms: u8) -> Self {
        Self {
            live: broadcast::channel(16).0,
            history: VecDeque::new(),
            preroll: 80usize.div_ceil(frame_ms.into()),
        }
    }
    fn publish(&mut self, packet: T) {
        if self.history.len() == self.preroll {
            self.history.pop_front();
        }
        self.history.push_back(packet.clone());
        let _ = self.live.send(packet);
    }
    fn subscribe(&self) -> (VecDeque<T>, broadcast::Receiver<T>) {
        (self.history.clone(), self.live.subscribe())
    }
}

type Bus = Arc<SyncMutex<Option<Fanout<Packet>>>>;
struct Group {
    bus: Bus,
    clock: PresentationClock,
    stop: Control,
    task: tokio::task::JoinHandle<()>,
}
#[derive(Default, Clone)]
pub(crate) struct Encoders(Arc<SyncMutex<HashMap<Config, Weak<Group>>>>);
pub(super) struct Feed {
    registry: Encoders,
    group: Arc<Group>,
    history: VecDeque<Packet>,
    live: broadcast::Receiver<Packet>,
}
impl Encoders {
    pub(super) fn attach(&self, audio: &Audio) -> Feed {
        let mut groups = self.0.lock().unwrap();
        groups.retain(|_, group| group.strong_count() > 0);
        if let Some(group) = groups.get(&audio.config).and_then(Weak::upgrade) {
            let joined = group.bus.lock().unwrap().as_ref().map(Fanout::subscribe);
            if let Some((history, live)) = joined {
                return Feed {
                    registry: self.clone(),
                    group,
                    history,
                    live,
                };
            }
            // The worker closed the bus and dropped its subscription before exit.
        }
        let fanout = Fanout::new(audio.config.frame_ms);
        let (history, live) = fanout.subscribe();
        let bus = Arc::new(SyncMutex::new(Some(fanout)));
        let stop = Control::pending();
        let runtime = tokio::runtime::Handle::current();
        let (source, config, pool, clock) = (
            audio.source.clone(),
            audio.config,
            audio.pool.clone(),
            audio.clock,
        );
        let worker_bus = bus.clone();
        let cancel = stop.clone();
        let task = tokio::task::spawn_blocking(move || {
            // Close all receivers on every exit, including a worker panic.
            struct Close(Bus);
            impl Drop for Close {
                fn drop(&mut self) {
                    self.0.lock().unwrap().take();
                }
            }
            let _close = Close(worker_bus.clone());
            let result: Result<(), Failure> = runtime.block_on(async {
                tokio::select! { biased;
                    _ = cancel.cancelled() => Ok(()),
                    r = async {
                        let mut subscription = source.subscribe(Tracks::Audio, SubscriptionLimits::default()).await?;
                        let mut delivery = subscription.recv().await?;
                        let mut codec = Transcoder::new_presented(&delivery, config, clock, &pool, Cancellation::default())?;
                        loop {
                            for packet in codec.push(&delivery)? {
                                worker_bus.lock().unwrap().as_mut().unwrap().publish(Arc::new(packet));
                            }
                            drop(delivery);
                            delivery = subscription.recv().await?;
                        }
                    } => r,
                }
            });
            if let Err(e) = result {
                warn!("live Opus encoder stopped: {e}");
            }
        });
        let group = Arc::new(Group {
            bus,
            clock,
            stop,
            task,
        });
        groups.insert(config, Arc::downgrade(&group));
        Feed {
            registry: self.clone(),
            group,
            history,
            live,
        }
    }
}
impl Feed {
    pub(super) fn clock(&self) -> PresentationClock {
        self.group.clock
    }
    pub(super) async fn recv(&mut self) -> Result<Packet, Failure> {
        if let Some(p) = self.history.pop_front() {
            return Ok(p);
        }
        Ok(self.live.recv().await?) // Lag is terminal for this viewer, never for siblings.
    }
    pub(super) async fn leave(self) {
        drop(self.live);
        drop(self.history);
        // A transient Weak::upgrade during attach must not become the last
        // group owner and drop a join handle outside this cleanup path.
        let last = {
            let _groups = self.registry.0.lock().unwrap();
            Arc::into_inner(self.group)
        };
        if let Some(group) = last {
            group.stop.revoke();
            let _ = group.task.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn matching_viewers_share_a_worker_and_stop_independently() {
        use amcrust::{
            ingest::CameraUrl,
            source::{Encoder, Limits, Registry, SourceKey},
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut registry = Registry::new(Limits::default()).unwrap();
        let source = registry
            .register(
                SourceKey {
                    camera: 1,
                    encoder: Encoder::Main,
                },
                CameraUrl::new(format!("rtsp://{}/stall", listener.local_addr().unwrap())).unwrap(),
            )
            .unwrap();
        let mut audio = Audio {
            clock: PresentationClock::new(),
            source,
            socket: tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            peer: "127.0.0.1:9".parse().unwrap(),
            key: vec![7; 30],
            ssrc: 1,
            identity: [1; 16],
            mtu: 1200,
            payload: 110,
            interval: Duration::from_secs(1),
            config: Config::default(),
            pool: Pool::new(2, 4 * 1024 * 1024).unwrap(),
            encoders: Encoders::default(),
        };
        let a = audio.encoders.attach(&audio);
        let b = audio.encoders.attach(&audio);
        assert!(Arc::ptr_eq(&a.group, &b.group));
        audio.config.bitrate *= 2;
        let c = audio.encoders.attach(&audio);
        assert!(!Arc::ptr_eq(&b.group, &c.group));
        a.leave().await;
        assert!(!b.group.stop.is_closed());
        let cancel = b.group.stop.clone();
        b.leave().await;
        assert!(cancel.is_closed());
        assert!(!c.group.stop.is_closed());
        c.leave().await;
        registry.shutdown().await.unwrap();
    }

    #[test]
    fn late_join_replays_whole_packets_then_live_without_a_gap() {
        for ms in [20, 40, 60] {
            let mut bus = Fanout::new(ms);
            let (_, mut first) = bus.subscribe();
            for i in 0..10 {
                bus.publish(i);
                assert_eq!(first.try_recv().unwrap(), i);
            }
            let (history, mut second) = bus.subscribe();
            let count = 80usize.div_ceil(ms.into());
            assert_eq!(history, (10 - count..10).collect::<VecDeque<_>>());
            bus.publish(10);
            assert_eq!(second.try_recv().unwrap(), 10);
            drop(second);
            assert_eq!(first.try_recv().unwrap(), 10);
        }
    }
}
