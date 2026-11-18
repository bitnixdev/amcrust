//! Maps Amcrest AI detection events onto the HomeKit motion sensor services.

use crate::{accessory::motion_service_iids, amcrest::CameraEvent, metrics::Metrics};
use hap::{HapType, pointer};
use log::{debug, info, warn};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    sync::broadcast,
    time::{Duration, Instant, sleep_until},
};

const MOTION_TIMEOUT: Duration = Duration::from_secs(90);

/// One replaceable deadline per sensor, owned by the mapper (no detached reset tasks).
#[derive(Default)]
struct MotionDeadlines([Option<Instant>; 2]);
impl MotionDeadlines {
    fn set(&mut self, vehicle: bool, detected: bool, now: Instant) {
        self.0[usize::from(vehicle)] = detected.then(|| now + MOTION_TIMEOUT);
    }
    fn next(&self) -> Option<(Instant, bool)> {
        self.0
            .into_iter()
            .enumerate()
            .filter_map(|(i, t)| t.map(|t| (t, i == 1)))
            .min()
    }
    fn active(&self) -> bool {
        self.0.iter().any(Option::is_some)
    }
}
pub struct MotionMapper {
    camera_name: String,
    accessory: pointer::Accessory,
    motion_active: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    deadlines: MotionDeadlines,
}
impl MotionMapper {
    pub fn new(
        camera_name: String,
        accessory: pointer::Accessory,
        motion_active: Arc<AtomicBool>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            camera_name,
            accessory,
            motion_active,
            metrics,
            deadlines: Default::default(),
        }
    }
    pub async fn run(mut self, mut rx: broadcast::Receiver<CameraEvent>) {
        loop {
            let next = self.deadlines.next();
            tokio::select! {
                biased;
                vehicle = async { let (at, vehicle) = next?; sleep_until(at).await; Some(vehicle) }, if next.is_some() => {
                    self.update(vehicle.expect("armed motion deadline"), false).await;
                }
                message = rx.recv() => match message {
                    Ok(event) => self.handle(event).await,
                    Err(broadcast::error::RecvError::Lagged(n)) => warn!("motion mapper lagged, skipped {n} events"),
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
    async fn handle(&mut self, event: CameraEvent) {
        debug!(
            "camera event index={} timestamp={}",
            event.index, event.timestamp
        );
        let vehicle = match event.code.as_str() {
            "SmartMotionVehicle" => true,
            "SmartMotionHuman" => false,
            _ => event.data.to_string().contains("Vehicle"),
        };
        self.metrics.event(vehicle, &event.action);
        let detected = match event.action.as_str() {
            "Start" => true,
            "Stop" => false,
            other => {
                warn!("unhandled event action: {other}");
                return;
            }
        };
        info!(
            "[{}] {} motion {} ({})",
            self.camera_name,
            if vehicle { "vehicle" } else { "person" },
            event.action,
            event.code
        );
        self.update(vehicle, detected).await;
    }
    async fn update(&mut self, vehicle: bool, detected: bool) {
        self.deadlines.set(vehicle, detected, Instant::now());
        let active = self.deadlines.active();
        self.motion_active.store(active, Ordering::SeqCst);
        self.metrics.motion_active(active);
        let (person_iid, vehicle_iid) = motion_service_iids();
        let iid = if vehicle { vehicle_iid } else { person_iid };
        let mut acc = self.accessory.lock().await;
        for service in acc.get_mut_services() {
            if service.get_id() == iid {
                if let Some(c) = service.get_mut_characteristic(HapType::MotionDetected) {
                    if let Err(e) = c.set_value(json!(detected)).await {
                        warn!("failed to set motion state: {e:?}");
                    }
                }
                return;
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn refresh_and_stop_replace_only_the_matching_deadline() {
        let t = Instant::now();
        let mut d = MotionDeadlines::default();
        d.set(false, true, t);
        d.set(true, true, t);
        for i in 1..1000 {
            d.set(false, true, t + Duration::from_millis(i));
        }
        assert_eq!(d.next(), Some((t + MOTION_TIMEOUT, true)));
        d.set(true, false, t + MOTION_TIMEOUT);
        assert!(d.active());
        assert_eq!(
            d.next(),
            Some((t + MOTION_TIMEOUT + Duration::from_millis(999), false))
        );
        d.set(false, false, t + MOTION_TIMEOUT);
        assert!(!d.active());
        assert!(d.next().is_none());
    }
}
