//! Lightweight Prometheus/VictoriaMetrics exposition and health server.

use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::hsv::recording::HsvState;

#[derive(Clone, Copy)]
pub enum ErrorSubsystem {
    EventStream,
    Snapshot,
    LiveStream,
    DataStream,
    Recording,
}

/// Process-wide counters and gauges. All labels are from fixed sets to avoid
/// unbounded Prometheus cardinality.
pub struct Metrics {
    started: Instant,
    event_person_start: AtomicU64,
    event_person_stop: AtomicU64,
    event_vehicle_start: AtomicU64,
    event_vehicle_stop: AtomicU64,
    event_other: AtomicU64,
    event_reconnects: AtomicU64,
    snapshot_requests: AtomicU64,
    snapshot_successes: AtomicU64,
    snapshot_deliveries: AtomicU64,
    snapshot_delivery_bytes: AtomicU64,
    snapshot_delivery_errors: AtomicU64,
    live_stream_starts: AtomicU64,
    recording_fragments: AtomicU64,
    recording_bytes: AtomicU64,
    errors_event_stream: AtomicU64,
    errors_snapshot: AtomicU64,
    errors_live_stream: AtomicU64,
    errors_data_stream: AtomicU64,
    errors_recording: AtomicU64,
    event_stream_connected: AtomicBool,
    live_connections: AtomicU64,
    data_stream_connections: AtomicU64,
    recording_stream_active: AtomicBool,
    motion_active: AtomicBool,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            event_person_start: AtomicU64::new(0),
            event_person_stop: AtomicU64::new(0),
            event_vehicle_start: AtomicU64::new(0),
            event_vehicle_stop: AtomicU64::new(0),
            event_other: AtomicU64::new(0),
            event_reconnects: AtomicU64::new(0),
            snapshot_requests: AtomicU64::new(0),
            snapshot_successes: AtomicU64::new(0),
            snapshot_deliveries: AtomicU64::new(0),
            snapshot_delivery_bytes: AtomicU64::new(0),
            snapshot_delivery_errors: AtomicU64::new(0),
            live_stream_starts: AtomicU64::new(0),
            recording_fragments: AtomicU64::new(0),
            recording_bytes: AtomicU64::new(0),
            errors_event_stream: AtomicU64::new(0),
            errors_snapshot: AtomicU64::new(0),
            errors_live_stream: AtomicU64::new(0),
            errors_data_stream: AtomicU64::new(0),
            errors_recording: AtomicU64::new(0),
            event_stream_connected: AtomicBool::new(false),
            live_connections: AtomicU64::new(0),
            data_stream_connections: AtomicU64::new(0),
            recording_stream_active: AtomicBool::new(false),
            motion_active: AtomicBool::new(false),
        })
    }

    pub fn event(&self, vehicle: bool, action: &str) {
        let counter = match (vehicle, action) {
            (false, "Start") => &self.event_person_start,
            (false, "Stop") => &self.event_person_stop,
            (true, "Start") => &self.event_vehicle_start,
            (true, "Stop") => &self.event_vehicle_stop,
            _ => &self.event_other,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn event_stream_connected(&self, connected: bool) {
        self.event_stream_connected
            .store(connected, Ordering::Relaxed);
    }

    pub fn event_stream_reconnect(&self) {
        self.event_reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot_request(&self) {
        self.snapshot_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot_success(&self) {
        self.snapshot_successes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot_delivered(&self, bytes: usize) {
        self.snapshot_deliveries.fetch_add(1, Ordering::Relaxed);
        self.snapshot_delivery_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn snapshot_delivery_failed(&self) {
        self.snapshot_delivery_errors
            .fetch_add(1, Ordering::Relaxed);
        self.errors_snapshot.fetch_add(1, Ordering::Relaxed);
    }

    pub fn live_stream_started(&self) {
        self.live_stream_starts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn recording_fragment(&self, bytes: usize) {
        self.recording_fragments.fetch_add(1, Ordering::Relaxed);
        self.recording_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn set_live_connections(&self, count: usize) {
        self.live_connections.store(count as u64, Ordering::Relaxed);
    }

    pub fn data_stream_opened(&self) {
        self.data_stream_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn data_stream_closed(&self) {
        self.data_stream_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .ok();
    }

    pub fn recording_stream_active(&self, active: bool) {
        self.recording_stream_active
            .store(active, Ordering::Relaxed);
    }

    pub fn motion_active(&self, active: bool) {
        self.motion_active.store(active, Ordering::Relaxed);
    }

    pub fn error(&self, subsystem: ErrorSubsystem) {
        let counter = match subsystem {
            ErrorSubsystem::EventStream => &self.errors_event_stream,
            ErrorSubsystem::Snapshot => &self.errors_snapshot,
            ErrorSubsystem::LiveStream => &self.errors_live_stream,
            ErrorSubsystem::DataStream => &self.errors_data_stream,
            ErrorSubsystem::Recording => &self.errors_recording,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn load(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    fn flag(value: &AtomicBool) -> u8 {
        u8::from(value.load(Ordering::Relaxed))
    }

    fn render(&self, recording_pipeline_running: bool) -> String {
        format!(
            concat!(
                "# HELP amcrust_build_info Build and version information.\n",
                "# TYPE amcrust_build_info gauge\n",
                "amcrust_build_info{{version=\"{}\"}} 1\n",
                "# HELP amcrust_uptime_seconds Seconds since the process started.\n",
                "# TYPE amcrust_uptime_seconds gauge\n",
                "amcrust_uptime_seconds {:.3}\n",
                "# HELP amcrust_camera_events_total Camera detection events received.\n",
                "# TYPE amcrust_camera_events_total counter\n",
                "amcrust_camera_events_total{{type=\"person\",action=\"start\"}} {}\n",
                "amcrust_camera_events_total{{type=\"person\",action=\"stop\"}} {}\n",
                "amcrust_camera_events_total{{type=\"vehicle\",action=\"start\"}} {}\n",
                "amcrust_camera_events_total{{type=\"vehicle\",action=\"stop\"}} {}\n",
                "amcrust_camera_events_total{{type=\"other\",action=\"other\"}} {}\n",
                "# HELP amcrust_errors_total Operational errors by subsystem.\n",
                "# TYPE amcrust_errors_total counter\n",
                "amcrust_errors_total{{subsystem=\"event_stream\"}} {}\n",
                "amcrust_errors_total{{subsystem=\"snapshot\"}} {}\n",
                "amcrust_errors_total{{subsystem=\"live_stream\"}} {}\n",
                "amcrust_errors_total{{subsystem=\"data_stream\"}} {}\n",
                "amcrust_errors_total{{subsystem=\"recording\"}} {}\n",
                "# HELP amcrust_connections_open Current open connections by type.\n",
                "# TYPE amcrust_connections_open gauge\n",
                "amcrust_connections_open{{type=\"event_stream\"}} {}\n",
                "amcrust_connections_open{{type=\"live_stream\"}} {}\n",
                "amcrust_connections_open{{type=\"data_stream\"}} {}\n",
                "# HELP amcrust_video_status Whether each video path is currently active.\n",
                "# TYPE amcrust_video_status gauge\n",
                "amcrust_video_status{{type=\"live\"}} {}\n",
                "amcrust_video_status{{type=\"recording_pipeline\"}} {}\n",
                "amcrust_video_status{{type=\"recording_delivery\"}} {}\n",
                "# HELP amcrust_motion_active Whether camera motion is currently active.\n",
                "# TYPE amcrust_motion_active gauge\n",
                "amcrust_motion_active {}\n",
                "# HELP amcrust_event_stream_reconnects_total Camera event-stream reconnects.\n",
                "# TYPE amcrust_event_stream_reconnects_total counter\n",
                "amcrust_event_stream_reconnects_total {}\n",
                "# HELP amcrust_snapshot_requests_total HomeKit snapshot requests.\n",
                "# TYPE amcrust_snapshot_requests_total counter\n",
                "amcrust_snapshot_requests_total {}\n",
                "# HELP amcrust_snapshot_successes_total Snapshot JPEGs generated successfully.\n",
                "# TYPE amcrust_snapshot_successes_total counter\n",
                "amcrust_snapshot_successes_total {}\n",
                "# HELP amcrust_snapshot_deliveries_total Snapshot responses written to controller sockets.\n",
                "# TYPE amcrust_snapshot_deliveries_total counter\n",
                "amcrust_snapshot_deliveries_total {}\n",
                "# HELP amcrust_snapshot_delivery_bytes_total Snapshot JPEG bytes written to controller sockets.\n",
                "# TYPE amcrust_snapshot_delivery_bytes_total counter\n",
                "amcrust_snapshot_delivery_bytes_total {}\n",
                "# HELP amcrust_snapshot_delivery_errors_total Snapshot responses that failed during socket delivery.\n",
                "# TYPE amcrust_snapshot_delivery_errors_total counter\n",
                "amcrust_snapshot_delivery_errors_total {}\n",
                "# HELP amcrust_live_stream_starts_total Live video streams started.\n",
                "# TYPE amcrust_live_stream_starts_total counter\n",
                "amcrust_live_stream_starts_total {}\n",
                "# HELP amcrust_recording_fragments_total Recording fragments produced.\n",
                "# TYPE amcrust_recording_fragments_total counter\n",
                "amcrust_recording_fragments_total {}\n",
                "# HELP amcrust_recording_bytes_total Recording fragment bytes produced.\n",
                "# TYPE amcrust_recording_bytes_total counter\n",
                "amcrust_recording_bytes_total {}\n",
            ),
            env!("CARGO_PKG_VERSION"),
            self.started.elapsed().as_secs_f64(),
            Self::load(&self.event_person_start),
            Self::load(&self.event_person_stop),
            Self::load(&self.event_vehicle_start),
            Self::load(&self.event_vehicle_stop),
            Self::load(&self.event_other),
            Self::load(&self.errors_event_stream),
            Self::load(&self.errors_snapshot),
            Self::load(&self.errors_live_stream),
            Self::load(&self.errors_data_stream),
            Self::load(&self.errors_recording),
            Self::flag(&self.event_stream_connected),
            Self::load(&self.live_connections),
            Self::load(&self.data_stream_connections),
            u8::from(Self::load(&self.live_connections) > 0),
            u8::from(recording_pipeline_running),
            Self::flag(&self.recording_stream_active),
            Self::flag(&self.motion_active),
            Self::load(&self.event_reconnects),
            Self::load(&self.snapshot_requests),
            Self::load(&self.snapshot_successes),
            Self::load(&self.snapshot_deliveries),
            Self::load(&self.snapshot_delivery_bytes),
            Self::load(&self.snapshot_delivery_errors),
            Self::load(&self.live_stream_starts),
            Self::load(&self.recording_fragments),
            Self::load(&self.recording_bytes),
        )
    }

    fn stderr_summary(&self) -> String {
        let person_events =
            Self::load(&self.event_person_start) + Self::load(&self.event_person_stop);
        let vehicle_events =
            Self::load(&self.event_vehicle_start) + Self::load(&self.event_vehicle_stop);
        let errors = Self::load(&self.errors_event_stream)
            + Self::load(&self.errors_snapshot)
            + Self::load(&self.errors_live_stream)
            + Self::load(&self.errors_data_stream)
            + Self::load(&self.errors_recording);
        format!(
            "amcrust stats: uptime={}s events(person={person_events}, vehicle={vehicle_events}, other={}) snapshots(generated={}/{}, delivered={}, delivery_errors={}) live_streams={} recording_fragments={} recording_bytes={} errors={} connections(event={}, live={}, data={}) motion={}",
            self.started.elapsed().as_secs(),
            Self::load(&self.event_other),
            Self::load(&self.snapshot_successes),
            Self::load(&self.snapshot_requests),
            Self::load(&self.snapshot_deliveries),
            Self::load(&self.snapshot_delivery_errors),
            Self::load(&self.live_stream_starts),
            Self::load(&self.recording_fragments),
            Self::load(&self.recording_bytes),
            errors,
            Self::flag(&self.event_stream_connected),
            Self::load(&self.live_connections),
            Self::load(&self.data_stream_connections),
            Self::flag(&self.motion_active),
        )
    }
}

/// Writes one compact, unconditional process summary to stderr every hour.
pub fn start_hourly_stderr_reporter(metrics: Arc<Metrics>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            eprintln!("{}", metrics.stderr_summary());
        }
    })
}

#[derive(Clone)]
struct HttpState {
    metrics: Arc<Metrics>,
    hsv: Arc<HsvState>,
}

pub fn start_server(
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    hsv: Arc<HsvState>,
) -> Result<(tokio::task::JoinHandle<()>, SocketAddr), Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind(addr)?;
    let bound_addr = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    let state = HttpState { metrics, hsv };
    let service = make_service_fn(move |_| {
        let state = state.clone();
        async move { Ok::<_, Infallible>(service_fn(move |request| handle(request, state.clone()))) }
    });
    let server = Server::from_tcp(listener)?.serve(service);
    let handle = tokio::spawn(async move {
        if let Err(error) = server.await {
            log::error!("metrics HTTP server error: {error}");
        }
    });
    Ok((handle, bound_addr))
}

async fn handle(request: Request<Body>, state: HttpState) -> Result<Response<Body>, Infallible> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/health") => {
            let recording_pipeline_running = state.hsv.recorder.is_running().await;
            let body = serde_json::json!({
                "status": "ok",
                "uptime_seconds": state.metrics.started.elapsed().as_secs_f64(),
                "event_stream_connected": state.metrics.event_stream_connected.load(Ordering::Relaxed),
                "live_connections": state.metrics.live_connections.load(Ordering::Relaxed),
                "recording_pipeline_running": recording_pipeline_running,
                "snapshot_requests": Metrics::load(&state.metrics.snapshot_requests),
                "snapshot_generated": Metrics::load(&state.metrics.snapshot_successes),
                "snapshot_delivered": Metrics::load(&state.metrics.snapshot_deliveries),
                "snapshot_delivery_errors": Metrics::load(&state.metrics.snapshot_delivery_errors),
            });
            typed_response(StatusCode::OK, "application/json", body.to_string())
        }
        (&Method::GET, "/metrics") => {
            let recording_pipeline_running = state.hsv.recorder.is_running().await;
            typed_response(
                StatusCode::OK,
                "text/plain; version=0.0.4; charset=utf-8",
                state.metrics.render(recording_pipeline_running),
            )
        }
        _ => typed_response(
            StatusCode::NOT_FOUND,
            "text/plain; charset=utf-8",
            "not found\n",
        ),
    };
    Ok(response)
}

fn typed_response(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<Body>,
) -> Response<Body> {
    let mut response = Response::new(body.into());
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

#[cfg(test)]
mod tests {
    use super::{HttpState, Metrics, handle};
    use crate::amcrest::{AmcrestClient, EncoderCapabilities};
    use crate::hsv::recording::HsvState;
    use hyper::{Body, Request, StatusCode};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn exposition_contains_prometheus_types_and_fixed_labels() {
        let metrics = Metrics::new();
        metrics.event(false, "Start");
        metrics.snapshot_request();
        metrics.snapshot_success();
        metrics.snapshot_delivered(4096);
        metrics.snapshot_delivery_failed();
        let output = metrics.render(true);
        assert!(output.contains("# TYPE amcrust_camera_events_total counter"));
        assert!(output.contains("amcrust_camera_events_total{type=\"person\",action=\"start\"} 1"));
        assert!(output.contains("amcrust_video_status{type=\"recording_pipeline\"} 1"));
        assert!(output.contains("amcrust_snapshot_successes_total 1"));
        assert!(output.contains("amcrust_snapshot_deliveries_total 1"));
        assert!(output.contains("amcrust_snapshot_delivery_bytes_total 4096"));
        assert!(output.contains("amcrust_snapshot_delivery_errors_total 1"));
    }

    #[tokio::test]
    async fn health_and_metrics_routes_return_scrapeable_responses() {
        let metrics = Metrics::new();
        let hsv = HsvState::load(
            "test-camera".into(),
            "/path/that/does/not/exist",
            AmcrestClient::new("camera.invalid".into(), "user".into(), "password".into()),
            Arc::new(AtomicBool::new(false)),
            metrics.clone(),
            EncoderCapabilities {
                main_resolutions: vec![(1920, 1080)],
                main_bitrate_range: Some((3, 8192)),
                ..Default::default()
            },
        );
        let state = HttpState { metrics, hsv };

        let health = handle(
            Request::get("/health").body(Body::empty()).unwrap(),
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        assert_eq!(health.headers()["content-type"], "application/json");
        let health_body = hyper::body::to_bytes(health.into_body()).await.unwrap();
        assert!(String::from_utf8_lossy(&health_body).contains("\"status\":\"ok\""));

        let response = handle(Request::get("/metrics").body(Body::empty()).unwrap(), state)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/plain; version=0.0.4")
        );
        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("amcrust_uptime_seconds"));
    }
}
