//! Maps Amcrest AI detection events onto the HomeKit motion sensor services.

use log::{info, warn};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::broadcast;
use tokio::time::{Duration, sleep};

use hap::{HapType, pointer};

use crate::accessory::motion_service_iids;
use crate::amcrest::CameraEvent;
use crate::metrics::Metrics;

/// Safety net: clear a motion sensor this long after the last Start if the
/// camera never sends a Stop.
const MOTION_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Clone)]
pub struct MotionMapper {
    accessory: pointer::Accessory,
    person_gen: Arc<AtomicU64>,
    vehicle_gen: Arc<AtomicU64>,
    person_active: Arc<AtomicBool>,
    vehicle_active: Arc<AtomicBool>,
    /// Shared "any motion active" flag, used by the HSV recording stream to
    /// decide when to mark end-of-stream.
    motion_active: Arc<std::sync::atomic::AtomicBool>,
    metrics: Arc<Metrics>,
}

impl MotionMapper {
    pub fn new(
        accessory: pointer::Accessory,
        motion_active: Arc<std::sync::atomic::AtomicBool>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            accessory,
            person_gen: Arc::new(AtomicU64::new(0)),
            vehicle_gen: Arc::new(AtomicU64::new(0)),
            person_active: Arc::new(AtomicBool::new(false)),
            vehicle_active: Arc::new(AtomicBool::new(false)),
            motion_active,
            metrics,
        }
    }

    pub async fn run(self, mut rx: broadcast::Receiver<CameraEvent>) {
        loop {
            match rx.recv().await {
                Ok(event) => self.handle(event).await,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("motion mapper lagged, skipped {n} events");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    async fn handle(&self, event: CameraEvent) {
        let (person_iid, vehicle_iid) = motion_service_iids();

        // SmartMotion events are explicitly typed; IVS cross-line/region events
        // carry the object type in their data payload.
        let is_vehicle = match event.code.as_str() {
            "SmartMotionVehicle" => true,
            "SmartMotionHuman" => false,
            _ => event.data.to_string().contains("Vehicle"),
        };
        let (iid, generation, active, other_active, label) = if is_vehicle {
            (
                vehicle_iid,
                &self.vehicle_gen,
                &self.vehicle_active,
                &self.person_active,
                "vehicle",
            )
        } else {
            (
                person_iid,
                &self.person_gen,
                &self.person_active,
                &self.vehicle_active,
                "person",
            )
        };
        self.metrics.event(is_vehicle, &event.action);

        match event.action.as_str() {
            "Start" => {
                info!("{} motion started ({})", label, event.code);
                active.store(true, Ordering::SeqCst);
                self.motion_active.store(true, Ordering::SeqCst);
                self.metrics.motion_active(true);
                let generation_at_start = generation.fetch_add(1, Ordering::SeqCst) + 1;
                set_motion(&self.accessory, iid, true).await;

                // Schedule the safety-net reset.
                let accessory = self.accessory.clone();
                let generation = generation.clone();
                let active = active.clone();
                let other_active = other_active.clone();
                let motion_active = self.motion_active.clone();
                let metrics = self.metrics.clone();
                tokio::spawn(async move {
                    sleep(MOTION_TIMEOUT).await;
                    if generation.load(Ordering::SeqCst) == generation_at_start {
                        active.store(false, Ordering::SeqCst);
                        motion_active.store(other_active.load(Ordering::SeqCst), Ordering::SeqCst);
                        metrics.motion_active(other_active.load(Ordering::SeqCst));
                        set_motion(&accessory, iid, false).await;
                    }
                });
            }
            "Stop" => {
                info!("{} motion stopped ({})", label, event.code);
                active.store(false, Ordering::SeqCst);
                self.motion_active
                    .store(other_active.load(Ordering::SeqCst), Ordering::SeqCst);
                self.metrics
                    .motion_active(other_active.load(Ordering::SeqCst));
                generation.fetch_add(1, Ordering::SeqCst);
                set_motion(&self.accessory, iid, false).await;
            }
            other => warn!("unhandled event action: {other}"),
        }
    }
}

async fn set_motion(accessory: &pointer::Accessory, service_iid: u64, detected: bool) {
    let mut acc = accessory.lock().await;
    for service in acc.get_mut_services() {
        if service.get_id() == service_iid {
            if let Some(c) = service.get_mut_characteristic(HapType::MotionDetected) {
                if let Err(e) = c.set_value(json!(detected)).await {
                    warn!("failed to set motion state: {e:?}");
                }
            }
            return;
        }
    }
}
