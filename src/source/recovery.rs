//! Bounded recovery observations, not playback readiness or retry scheduling.
use super::{Terminal, ingest};
use std::time::Instant;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPhase {
    #[default]
    Idle,
    Opening,
    Acquiring,
    Backoff,
    Cooldown,
    Disabled,
    Retiring,
    Shutdown,
    Failed,
}
impl RecoveryPhase {
    pub const ALL: [Self; 9] = [
        Self::Idle,
        Self::Opening,
        Self::Acquiring,
        Self::Backoff,
        Self::Cooldown,
        Self::Disabled,
        Self::Retiring,
        Self::Shutdown,
        Self::Failed,
    ];
    pub fn label(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Opening => "opening",
            Self::Acquiring => "acquiring",
            Self::Backoff => "backoff",
            Self::Cooldown => "cooldown",
            Self::Disabled => "disabled",
            Self::Retiring => "retiring",
            Self::Shutdown => "shutdown",
            Self::Failed => "failed",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultKind {
    Native,
    Deadline,
    Budget,
    Configuration,
    Clock,
    Unsupported,
    Worker,
    Other,
}
impl FaultKind {
    pub const ALL: [Self; 8] = [
        Self::Native,
        Self::Deadline,
        Self::Budget,
        Self::Configuration,
        Self::Clock,
        Self::Unsupported,
        Self::Worker,
        Self::Other,
    ];
    pub fn label(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Deadline => "deadline",
            Self::Budget => "budget",
            Self::Configuration => "configuration",
            Self::Clock => "clock",
            Self::Unsupported => "unsupported",
            Self::Worker => "worker",
            Self::Other => "other",
        }
    }
    pub(super) fn from_terminal(reason: Terminal) -> Self {
        match reason {
            Terminal::Source(ingest::Error::Native { .. }) => Self::Native,
            Terminal::Source(ingest::Error::Deadline) => Self::Deadline,
            Terminal::Budget
            | Terminal::Source(ingest::Error::PacketLimit | ingest::Error::Allocation) => {
                Self::Budget
            }
            Terminal::ConfigurationChanged
            | Terminal::Source(
                ingest::Error::ConfigurationChanged | ingest::Error::InvalidConfiguration,
            ) => Self::Configuration,
            Terminal::Clock | Terminal::Source(ingest::Error::InvalidTimingMetadata) => Self::Clock,
            Terminal::Unsupported | Terminal::Source(ingest::Error::UnsupportedLayout) => {
                Self::Unsupported
            }
            Terminal::WorkerFailed => Self::Worker,
            _ => Self::Other,
        }
    }
}
#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct RecoverySnapshot {
    pub phase: RecoveryPhase,
    pub consecutive_failures: u32,
    /// Delay barrier only; opening still requires fresh eligible demand.
    pub retry_remaining_ms: u64,
    pub last_fault: Option<FaultKind>,
    pub cooldowns: u64,
    /// Circuit cooldown resets, not successful media/controller recoveries.
    pub cooldown_resets: u64,
}
#[derive(Default)]
pub(super) struct Record {
    pub snapshot: RecoverySnapshot,
    pub retry_at: Option<Instant>,
}
impl Record {
    pub fn snapshot(&self) -> RecoverySnapshot {
        RecoverySnapshot {
            retry_remaining_ms: self.retry_at.map_or(0, |deadline| {
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis()
                    .min(u64::MAX as u128) as u64
            }),
            ..self.snapshot
        }
    }
}
