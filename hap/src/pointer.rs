use futures::lock::Mutex;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

use crate::{accessory, event, storage};

pub type ControllerId = Arc<RwLock<Option<Uuid>>>;

pub type EventEmitter = Arc<Mutex<event::EventEmitter>>;

pub type EventSubscriptions = Arc<Mutex<Vec<(u64, u64)>>>;

pub type AccessoryDatabase = Arc<Mutex<storage::accessory_database::AccessoryDatabase>>;

pub type Accessory = Arc<Mutex<Box<dyn accessory::HapAccessory>>>;

pub type Storage = Arc<Mutex<Box<dyn storage::Storage>>>;

pub type Config = Arc<Mutex<crate::Config>>;

pub type MdnsResponder = Arc<Mutex<crate::transport::mdns::MdnsResponder>>;

/// Sticky snapshot-epoch permission, including escaped/copied response bytes.
/// Revocation serializes with the final nonblocking socket write.
#[derive(Clone, Debug)]
pub struct SnapshotPermit(Arc<SnapshotPermission>);
#[derive(Debug)]
struct SnapshotPermission {
    current: RwLock<bool>,
    retired: tokio::sync::watch::Sender<bool>,
}
impl SnapshotPermit {
    pub fn new() -> Self {
        Self(Arc::new(SnapshotPermission {
            current: RwLock::new(true),
            retired: tokio::sync::watch::channel(false).0,
        }))
    }
    pub fn revoke(&self) {
        *self.0.current.write().unwrap_or_else(|e| e.into_inner()) = false;
        self.0.retired.send_replace(true);
    }
    pub fn current(&self) -> bool {
        self.0.current.read().map(|v| *v).unwrap_or(false)
    }
    pub fn write<T>(&self, syscall: impl FnOnce() -> T) -> std::io::Result<T> {
        let current = self.0.current.read().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "snapshot permission poisoned",
            )
        })?;
        if !*current {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "snapshot retired",
            ));
        }
        Ok(syscall())
    }
    pub fn retired(&self) -> futures::future::BoxFuture<'static, ()> {
        let mut retired = self.0.retired.subscribe();
        Box::pin(async move {
            if !*retired.borrow() {
                let _ = retired.changed().await;
            }
        })
    }
}
impl Default for SnapshotPermit {
    fn default() -> Self {
        Self::new()
    }
}

/// Outcome of a snapshot request.
pub enum SnapshotResult {
    /// Serve the image.
    Jpeg {
        image: Vec<u8>,
        permission: SnapshotPermit,
        camera_name: String,
        source_generation: u64,
        output_fingerprint: u64,
    },
    /// Reject with `207 Multi-Status` and the given HAP status code
    /// (e.g. -70401 insufficient privileges, -70412 not allowed in current state).
    HapStatus(i32),
}

/// Future returned by a snapshot handler.
pub type SnapshotFuture = futures::future::BoxFuture<
    'static,
    std::result::Result<SnapshotResult, Box<dyn std::error::Error + Send + Sync>>,
>;

/// Handler invoked for HAP `POST /resource` image requests with the requested
/// width, height, and the optional secure-video `reason` property
/// (0 = periodic snapshot, 1 = event snapshot).
pub type SnapshotHandler =
    Arc<Mutex<Option<Box<dyn FnMut(u32, u32, Option<i64>) -> SnapshotFuture + Send + Sync>>>>;

/// Result of writing a complete snapshot response to the controller socket.
#[derive(Clone, Copy, Debug)]
pub enum SnapshotDelivery {
    Delivered { bytes: usize },
    Failed,
}

/// Observer invoked after encrypted snapshot delivery succeeds or fails.
pub type SnapshotDeliveryHandler = Arc<RwLock<Option<Arc<dyn Fn(SnapshotDelivery) + Send + Sync>>>>;

/// Slot holding the pair-verify shared secret of the session currently
/// performing a characteristics write. Lets characteristic write handlers
/// (e.g. Setup Data Stream Transport) derive session-bound keys.
pub type SharedSecretSlot = Arc<std::sync::RwLock<Option<[u8; 32]>>>;
