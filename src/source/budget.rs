//! Read-only views of existing admission semaphores. Logical charges are not RSS.
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::Semaphore;

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct RegistryBudgetSnapshot {
    pub charged_bytes: usize,
    pub byte_limit: usize,
    /// Includes retired subscriptions retained by callers, not just active clients.
    pub reserved_subscribers: usize,
    pub subscriber_limit: usize,
    pub subscription_budget_refusals: u64,
}
/// Observes budgets without retaining supervisors or granting admission.
#[derive(Clone)]
pub struct RegistryBudgetObserver {
    bytes: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    byte_limit: usize,
    subscriber_limit: usize,
    refusals: Arc<AtomicU64>,
}
impl RegistryBudgetObserver {
    pub(super) fn new(
        bytes: Arc<Semaphore>,
        slots: Arc<Semaphore>,
        byte_limit: usize,
        subscriber_limit: usize,
        refusals: Arc<AtomicU64>,
    ) -> Self {
        Self {
            bytes,
            slots,
            byte_limit,
            subscriber_limit,
            refusals,
        }
    }
    /// Fields are sampled independently, not as an atomic whole-process ledger.
    pub fn snapshot(&self) -> RegistryBudgetSnapshot {
        RegistryBudgetSnapshot {
            charged_bytes: self.byte_limit - self.bytes.available_permits(),
            byte_limit: self.byte_limit,
            reserved_subscribers: self.subscriber_limit - self.slots.available_permits(),
            subscriber_limit: self.subscriber_limit,
            subscription_budget_refusals: self.refusals.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    #[test]
    fn aggregate_charges_survive_registry_and_partial_acquisition_rolls_back() {
        let mut limits = Limits {
            bytes: 1024,
            bytes_per_source: 1024,
            ..Default::default()
        };
        limits.reader.packet_bytes = 1024;
        let registry = Registry::new(limits).unwrap();
        let observer = registry.budget_observer();
        let source = Arc::new(Semaphore::new(1024));
        let other = Arc::new(Semaphore::new(1024));
        let held = charge((), 512, &source, &registry.bytes).unwrap();
        assert_eq!(observer.snapshot().charged_bytes, 512);
        assert!(matches!(
            charge((), 1024, &source, &registry.bytes),
            Err(ingest::Error::PacketLimit)
        ));
        assert!(matches!(
            charge((), 1024, &other, &registry.bytes),
            Err(ingest::Error::PacketLimit)
        ));
        assert_eq!(other.available_permits(), 1024); // aggregate refusal releases local charge
        assert_eq!(observer.snapshot().charged_bytes, 512);
        drop(registry);
        assert_eq!(observer.snapshot().charged_bytes, 512); // escaped ownership, not registry lifetime
        drop(held);
        assert_eq!(observer.snapshot().charged_bytes, 0);
        assert_eq!(source.available_permits(), 1024);
    }
}
