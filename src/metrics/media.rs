//! Fixed-role application telemetry. Logical charges overlap; never sum them as RSS.
use crate::{hsv::fmp4::RecordingResources, stream::StreamResources};
use std::fmt::Write;

pub(super) fn render(streams: &StreamResources, recording: &RecordingResources) -> String {
    let mut out = String::new();
    for (name, help, kind) in [
        (
            "source_registered",
            "Whether this encoder role has a registered supervisor.",
            "gauge",
        ),
        (
            "source_active",
            "Whether the source generation is active; not playback readiness.",
            "gauge",
        ),
        (
            "source_generation",
            "Current or latest generation ordinal.",
            "gauge",
        ),
        (
            "source_open_attempts_total",
            "Acquisition open attempts including failures.",
            "counter",
        ),
        (
            "source_faults_total",
            "Supervisor faults; not controller reconnect success.",
            "counter",
        ),
        (
            "source_subscribers",
            "Current source subscriptions.",
            "gauge",
        ),
        (
            "source_charged_bytes",
            "Logical source bytes, including escaped charged ownership; not RSS.",
            "gauge",
        ),
        (
            "source_byte_limit",
            "Configured logical per-source byte limit.",
            "gauge",
        ),
        (
            "source_oldest_delivery_seconds",
            "Sampled oldest queued or held delivery age for active subscriptions.",
            "gauge",
        ),
        (
            "source_skipped_initial_clocks_total",
            "Initial packets skipped for missing source clocks.",
            "counter",
        ),
        (
            "media_reserved_jobs",
            "Reserved job slots including escaped output ownership; not running threads.",
            "gauge",
        ),
        (
            "media_job_limit",
            "Configured logical job-slot limit.",
            "gauge",
        ),
        (
            "media_reserved_bytes",
            "Conservative logical job reservations; not allocator bytes or RSS.",
            "gauge",
        ),
        (
            "media_byte_limit",
            "Configured logical reservation limit.",
            "gauge",
        ),
        (
            "live_owner_snapshot_available",
            "Whether live owner counts could be sampled without waiting.",
            "gauge",
        ),
        (
            "live_owned_sessions",
            "Retained preparations or live owners, including pending expiry cleanup and teardown.",
            "gauge",
        ),
        (
            "recording_history_snapshot_available",
            "Whether recording history could be sampled without waiting.",
            "gauge",
        ),
        (
            "recording_history_bytes",
            "Encoded payload bytes retained in history; overlaps source and codec ownership.",
            "gauge",
        ),
        (
            "recording_history_entries",
            "Encoded video/AAC entries retained in history.",
            "gauge",
        ),
        (
            "recording_history_end_span_seconds",
            "Difference between oldest and latest retained frame ends.",
            "gauge",
        ),
        (
            "recording_clip_owners",
            "Retained recording clip owners including completed/tearing-down workers.",
            "gauge",
        ),
    ] {
        writeln!(
            out,
            "# HELP amcrust_{name} {help}\n# TYPE amcrust_{name} {kind}"
        )
        .unwrap();
    }
    for (name, kind, help) in [
        (
            "recovery_phase",
            "gauge",
            "Observed supervisor phase; acquiring is not playback readiness.",
        ),
        (
            "last_fault",
            "gauge",
            "Last completed fault category, retained across resets; none before first fault.",
        ),
        (
            "consecutive_failures",
            "gauge",
            "Current circuit failure count.",
        ),
        (
            "retry_remaining_seconds",
            "gauge",
            "Remaining retry delay barrier; opening still requires eligible demand.",
        ),
        (
            "cooldowns_total",
            "counter",
            "Exhausted retry bursts entering cooldown.",
        ),
        (
            "cooldown_resets_total",
            "counter",
            "Timed circuit resets, not media or controller recovery successes.",
        ),
    ] {
        writeln!(
            out,
            "# HELP amcrust_source_{name} {help}\n# TYPE amcrust_source_{name} {kind}"
        )
        .unwrap();
    }
    for (label, snapshot) in [
        ("main", Some(&streams.main)),
        ("sub_stream2", streams.sub_stream2.as_ref()),
    ] {
        writeln!(
            out,
            "amcrust_source_registered{{encoder=\"{label}\"}} {}",
            u8::from(snapshot.is_some())
        )
        .unwrap();
        let Some(s) = snapshot else { continue };
        for phase in amcrust::source::RecoveryPhase::ALL {
            writeln!(
                out,
                "amcrust_source_recovery_phase{{encoder=\"{label}\",phase=\"{}\"}} {}",
                phase.label(),
                u8::from(s.recovery.phase == phase)
            )
            .unwrap();
        }
        for fault in
            std::iter::once(None).chain(amcrust::source::FaultKind::ALL.into_iter().map(Some))
        {
            writeln!(
                out,
                "amcrust_source_last_fault{{encoder=\"{label}\",kind=\"{}\"}} {}",
                fault.map_or("none", |f| f.label()),
                u8::from(s.recovery.last_fault == fault)
            )
            .unwrap();
        }
        writeln!(
            out,
            "amcrust_source_retry_remaining_seconds{{encoder=\"{label}\"}} {:.3}",
            s.recovery.retry_remaining_ms as f64 / 1000.0
        )
        .unwrap();
        for (name, value) in [
            ("active", u64::from(s.active)),
            (
                "consecutive_failures",
                u64::from(s.recovery.consecutive_failures),
            ),
            ("cooldowns_total", s.recovery.cooldowns),
            ("cooldown_resets_total", s.recovery.cooldown_resets),
            ("generation", s.generation),
            ("open_attempts_total", s.open_attempts),
            ("faults_total", s.faults),
            ("subscribers", s.subscribers as u64),
            ("charged_bytes", s.charged_bytes as u64),
            ("byte_limit", s.byte_limit as u64),
            ("skipped_initial_clocks_total", s.skipped_initial_clocks),
        ] {
            writeln!(out, "amcrust_source_{name}{{encoder=\"{label}\"}} {value}").unwrap();
        }
        writeln!(
            out,
            "amcrust_source_oldest_delivery_seconds{{encoder=\"{label}\"}} {:.3}",
            s.oldest_delivery_ms as f64 / 1000.0
        )
        .unwrap();
    }
    for (label, pool) in [
        ("live_opus", &streams.live_opus),
        ("recording_aac", &recording.aac),
        ("recording_mux", &recording.mux),
    ] {
        for (name, value) in [
            ("reserved_jobs", pool.reserved_jobs),
            ("job_limit", pool.job_limit),
            ("reserved_bytes", pool.reserved_bytes),
            ("byte_limit", pool.byte_limit),
        ] {
            writeln!(out, "amcrust_media_{name}{{pool=\"{label}\"}} {value}").unwrap();
        }
    }
    writeln!(
        out,
        "amcrust_live_owner_snapshot_available {}",
        u8::from(streams.owners.is_some())
    )
    .unwrap();
    for (name, value) in [
        ("live_setup_byte_limit", streams.limits.setup_bytes as u64),
        ("live_session_limit", streams.limits.live_sessions as u64),
        (
            "live_prepared_limit",
            streams.limits.prepared_sessions as u64,
        ),
        (
            "live_prepared_lifetime_seconds",
            streams.limits.prepared_lifetime_seconds,
        ),
    ] {
        writeln!(out, "# HELP amcrust_{name} Configured live admission limit.\n# TYPE amcrust_{name} gauge\namcrust_{name} {value}").unwrap();
    }
    for name in [
        "live_setup_busy_total",
        "live_start_busy_total",
        "live_prepared_expired_total",
    ] {
        writeln!(out, "# HELP amcrust_{name} Live admission busy refusals or expired preparations.\n# TYPE amcrust_{name} counter").unwrap();
    }
    writeln!(out, "# HELP amcrust_live_owned_session_ids Retained live session identifiers, counted once across both tracks.\n# TYPE amcrust_live_owned_session_ids gauge").unwrap();
    if let Some(owners) = &streams.owners {
        writeln!(out, "amcrust_live_owned_session_ids {}\namcrust_live_setup_busy_total {}\namcrust_live_start_busy_total {}\namcrust_live_prepared_expired_total {}", owners.live_sessions, owners.setup_busy, owners.start_busy, owners.expired_prepared).unwrap();
        for (label, value) in [
            ("prepared", owners.prepared_sessions),
            ("video", owners.video),
            ("audio", owners.audio),
        ] {
            writeln!(
                out,
                "amcrust_live_owned_sessions{{kind=\"{label}\"}} {value}"
            )
            .unwrap();
        }
    }
    writeln!(
        out,
        "amcrust_recording_history_snapshot_available {}",
        u8::from(recording.history.is_some())
    )
    .unwrap();
    if let Some(history) = &recording.history {
        writeln!(out, "amcrust_recording_history_bytes {}\namcrust_recording_history_entries {}\namcrust_recording_history_end_span_seconds {:.6}\namcrust_recording_clip_owners {}",
            history.bytes, history.entries, history.end_span_seconds, history.clip_owners).unwrap();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hsv::fmp4::HistoryResources;
    use crate::stream::LiveOwners;
    use amcrust::{media::PoolSnapshot, source::Snapshot};

    #[test]
    fn logical_observations_have_fixed_labels_and_explicit_unavailability() {
        let source = Snapshot {
            active: true,
            generation: 3,
            open_attempts: 5,
            faults: 2,
            subscribers: 4,
            skipped_initial_clocks: 1,
            charged_bytes: 1234,
            byte_limit: 8192,
            oldest_delivery_ms: 1250,
            recovery: Default::default(),
        };
        let pool = PoolSnapshot {
            reserved_jobs: 1,
            job_limit: 2,
            reserved_bytes: 4096,
            byte_limit: 8192,
        };
        let mut streams = StreamResources {
            main: source,
            sub_stream2: Some(source),
            live_opus: pool,
            limits: Default::default(),
            owners: Some(LiveOwners {
                prepared_sessions: 2,
                video: 3,
                audio: 1,
                live_sessions: 3,
                setup_busy: 2,
                start_busy: 1,
                expired_prepared: 4,
            }),
        };
        let mut recording = RecordingResources {
            aac: pool,
            mux: pool,
            history: Some(HistoryResources {
                pipeline_running: true,
                bytes: 456,
                entries: 7,
                end_span_seconds: 2.25,
                clip_owners: 1,
            }),
        };
        let output = render(&streams, &recording);
        assert!(output.contains("# TYPE amcrust_source_open_attempts_total counter"));
        assert!(output.contains("amcrust_source_faults_total{encoder=\"main\"} 2"));
        assert!(
            output
                .contains("amcrust_source_oldest_delivery_seconds{encoder=\"sub_stream2\"} 1.250")
        );
        assert!(output.contains("amcrust_media_reserved_bytes{pool=\"recording_mux\"} 4096"));
        assert!(output.contains("amcrust_recording_history_bytes 456"));
        assert!(output.contains("amcrust_live_start_busy_total 1"));
        assert!(
            output.contains("amcrust_source_recovery_phase{encoder=\"main\",phase=\"idle\"} 1")
        );
        assert!(
            output.contains("amcrust_source_recovery_phase{encoder=\"main\",phase=\"cooldown\"} 0")
        );
        assert!(output.contains("amcrust_source_last_fault{encoder=\"main\",kind=\"none\"} 1"));
        assert!(output.contains("amcrust_live_prepared_expired_total 4"));
        assert!(output.contains("amcrust_live_session_limit 8"));
        assert!(output.contains("amcrust_live_owned_session_ids 3"));
        assert!(output.contains("amcrust_live_owned_sessions{kind=\"prepared\"} 2"));
        streams.owners = None;
        recording.history = None;
        let output = render(&streams, &recording);
        assert!(output.contains("amcrust_live_owner_snapshot_available 0"));
        assert!(output.contains("amcrust_recording_history_snapshot_available 0"));
        assert!(!output.contains("\namcrust_recording_history_bytes "));
        assert!(!output.contains("\namcrust_live_owned_sessions{"));
        assert!(
            super::super::Metrics::new()
                .render(None)
                .contains("amcrust_video_status{type=\"recording_pipeline\"} NaN")
        );
    }
}
