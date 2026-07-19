//! Maps Amcrest AI detection events onto the HomeKit motion sensor services.

use log::{info, warn};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;
use tokio::time::{Duration, sleep};

use hap::{HapType, pointer};

use crate::accessory::motion_service_iids;
use crate::amcrest::CameraEvent;

/// Safety net: clear a motion sensor this long after the last Start if the
/// camera never sends a Stop.
const MOTION_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Clone)]
pub struct MotionMapper {
    accessory: pointer::Accessory,
    person_gen: Arc<AtomicU64>,
    vehicle_gen: Arc<AtomicU64>,
    /// Shared "any motion active" flag, used by the HSV recording stream to
    /// decide when to mark end-of-stream.
    motion_active: Arc<std::sync::atomic::AtomicBool>,
}

impl MotionMapper {
    pub fn new(
        accessory: pointer::Accessory,
        motion_active: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            accessory,
            person_gen: Arc::new(AtomicU64::new(0)),
            vehicle_gen: Arc::new(AtomicU64::new(0)),
            motion_active,
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
        let (iid, generation, label) = if is_vehicle {
            (vehicle_iid, &self.vehicle_gen, "vehicle")
        } else {
            (person_iid, &self.person_gen, "person")
        };

        match event.action.as_str() {
            "Start" => {
                info!("{} motion started ({})", label, event.code);
                self.motion_active.store(true, Ordering::SeqCst);
                let generation_at_start = generation.fetch_add(1, Ordering::SeqCst) + 1;
                set_motion(&self.accessory, iid, true).await;

                // Schedule the safety-net reset.
                let accessory = self.accessory.clone();
                let generation = generation.clone();
                let motion_active = self.motion_active.clone();
                tokio::spawn(async move {
                    sleep(MOTION_TIMEOUT).await;
                    if generation.load(Ordering::SeqCst) == generation_at_start {
                        motion_active.store(false, Ordering::SeqCst);
                        set_motion(&accessory, iid, false).await;
                    }
                });
            }
            "Stop" => {
                info!("{} motion stopped ({})", label, event.code);
                self.motion_active.store(false, Ordering::SeqCst);
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
