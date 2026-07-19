mod accessory;
mod amcrest;
mod hsv;
mod metrics;
mod motion;
mod srtp;
mod stream;
mod tlv8;

use clap::Parser;
use futures::FutureExt;
use log::{debug, info, warn};
use rand::Rng;
use tokio::sync::broadcast;

use hap::{
    Config, Pin,
    accessory::AccessoryCategory,
    server::{IpServer, Server},
    storage::{FileStorage, Storage},
};

use accessory::CameraAccessory;
use amcrest::{AmcrestClient, CameraEvent, SnapshotCache};
use hsv::hds::HdsServer;
use hsv::recording::HsvState;
use metrics::{ErrorSubsystem, Metrics};
use motion::MotionMapper;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use stream::StreamManager;

/// A single Amcrest camera bridged to HomeKit as a camera/video accessory.
/// Run one instance per camera.
#[derive(Parser)]
struct Args {
    /// Camera name (also the HomeKit accessory name)
    #[arg(long, env = "CAMERA_NAME")]
    name: String,

    /// Camera IP or hostname
    #[arg(long, env = "CAMERA_HOST")]
    host: String,

    /// Camera API username
    #[arg(long, env = "AMCREST_USERNAME")]
    username: String,

    /// Camera API password
    #[arg(long, env = "AMCREST_PASSWORD")]
    password: String,

    /// HAP server port. Defaults to a free port chosen on first run and kept
    /// thereafter, so multiple instances coexist on one machine.
    #[arg(long, env = "HAP_PORT")]
    port: Option<u16>,

    /// Override the generated HomeKit setup PIN (8 digits)
    #[arg(long, env = "HAP_PIN")]
    pin: Option<String>,

    /// Directory for HomeKit pairing state (per-camera subdirectory is created)
    #[arg(long, env = "DATA_DIR", default_value = "./data")]
    data_dir: String,

    /// RTSP stream subtype to serve to HomeKit (0 = main, 1/2 = sub streams)
    #[arg(long, env = "RTSP_SUBTYPE", default_value = "2")]
    rtsp_subtype: u8,

    /// Transcode and send camera audio for live view (AAC → Opus)
    #[arg(long, env = "AUDIO", default_value = "true")]
    audio: bool,

    /// HTTP port for Prometheus metrics and health checks
    #[arg(long, env = "METRICS_PORT", default_value = "0")]
    metrics_port: u16,

    /// Save the most recently served snapshot as <camera-name>.jpg
    #[arg(long, env = "SAVE_SNAPSHOTS")]
    save_snapshots: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn,amcrust=info,libmdns=error"),
    )
    .init();

    let args = Args::parse();
    let metrics = Metrics::new();

    let camera = AmcrestClient::new(
        args.host.clone(),
        args.username.clone(),
        args.password.clone(),
    );

    // Best-effort: fetch the camera model for the accessory information.
    let model = match camera.get_device_type().await {
        Ok(model) => model,
        Err(e) => {
            warn!("could not query camera model: {e}");
            "IP Camera".to_string()
        }
    };
    info!("camera {} at {} ({model})", args.name, args.host);

    // Make sure the live-view substream is enabled and configured; cameras
    // ship with sub stream 2 disabled, which breaks HomeKit live view.
    if let Err(e) = camera.ensure_live_substream(args.rtsp_subtype).await {
        warn!("could not verify live substream config: {e}");
    }
    if let Err(e) = camera.ensure_audio_profile().await {
        warn!("could not verify camera audio profile: {e}");
    }
    if let Err(e) = camera.ensure_overlay_profile().await {
        warn!("could not verify camera overlay profile: {e}");
    }
    let mut storage = FileStorage::new(&format!("{}/{}", args.data_dir, args.name)).await?;

    let config = match storage.load_config().await {
        Ok(mut config) => {
            config.redetermine_local_ip();
            if let Some(pin) = args.pin.as_deref() {
                config.pin = parse_pin(pin)?;
            } else if config.pin == legacy_default_pin() {
                config.pin = generate_pin();
                info!("replaced legacy default HomeKit setup code with a unique generated code");
            }
            if let Some(port) = args.port {
                config.port = port;
            } else if std::net::TcpListener::bind((config.host, config.port)).is_err() {
                // Stored port is taken (e.g. by another instance); move to a
                // free one — controllers re-resolve the port via mDNS.
                let port = find_free_port(config.host)?;
                warn!(
                    "stored port {} is unavailable, moving to {port}",
                    config.port
                );
                config.port = port;
            }
            storage.save_config(&config).await?;
            config
        }
        Err(_) => {
            let mut config = Config {
                pin: match args.pin.as_deref() {
                    Some(pin) => parse_pin(pin)?,
                    None => generate_pin(),
                },
                name: args.name.clone(),
                category: AccessoryCategory::IpCamera,
                ..Default::default()
            };
            config.redetermine_local_ip();
            config.port = match args.port {
                Some(port) => port,
                None => find_free_port(config.host)?,
            };
            storage.save_config(&config).await?;
            config
        }
    };

    // Fail fast (and cleanly) if our port is taken, e.g. by another instance.
    if let Err(e) = std::net::TcpListener::bind((config.host, config.port)) {
        log::error!(
            "port {} on {} is unavailable: {e}; is another instance using it? (override with --port)",
            config.port,
            config.host
        );
        std::process::exit(1);
    }

    let local_ip = config.host;
    let hap_port = config.port;
    let setup_code = format_setup_code(&config.pin);

    let streams = StreamManager::new(
        camera.rtsp_url(args.rtsp_subtype),
        camera.rtsp_url(0),
        args.audio,
        local_ip,
        metrics.clone(),
    );

    // HomeKit Secure Video state, recorder, and data stream server.
    let motion_active = Arc::new(AtomicBool::new(false));
    let hsv_state = HsvState::load(
        &format!("{}/{}", args.data_dir, args.name),
        camera.clone(),
        motion_active.clone(),
        metrics.clone(),
    );
    let hds = HdsServer::new(hsv_state.clone(), metrics.clone());

    let requested_metrics_addr = std::net::SocketAddr::from(([0, 0, 0, 0], args.metrics_port));
    let (_metrics_server, metrics_addr) =
        metrics::start_server(requested_metrics_addr, metrics.clone(), hsv_state.clone())?;
    let _stats_reporter = metrics::start_hourly_stderr_reporter(metrics.clone());
    info!("health and metrics HTTP server on {metrics_addr}");

    let camera_accessory_probe = CameraAccessory::new(
        1,
        &args.name,
        &model,
        &streams,
        &hsv_state,
        &hds,
        Default::default(),
    )
    .await?;
    // Bump the HAP configuration number when the accessory's shape changed
    // (e.g. new services after an update), so paired controllers re-read it.
    let shape = serde_json::to_vec(&camera_accessory_probe)?;
    drop(camera_accessory_probe);
    let shape_hash = format!("{:x}", md5_like_hash(&shape));
    let hash_path = format!("{}/{}/accessory_shape", args.data_dir, args.name);
    let mut config = config;
    if std::fs::read_to_string(&hash_path).ok().as_deref() != Some(shape_hash.as_str()) {
        config.configuration_number += 1;
        info!(
            "accessory shape changed; configuration number → {}",
            config.configuration_number
        );
        storage.save_config(&config).await?;
        std::fs::write(&hash_path, &shape_hash).ok();
    }

    let server = IpServer::new(config, storage).await?;

    let camera_accessory = CameraAccessory::new(
        1,
        &args.name,
        &model,
        &streams,
        &hsv_state,
        &hds,
        server.shared_secret_slot(),
    )
    .await?;
    let accessory_ptr = server.add_accessory(camera_accessory).await?;

    // Restore the persisted encoder settings before resuming the recording
    // pipeline, so ffmpeg never attaches to a stale camera configuration.
    hsv_state.resume_recorder().await;
    // Encoder writes reset motion fields on some models. Apply detection last
    // even when HomeKit has not selected a recording configuration yet.
    if let Err(e) = camera.ensure_smart_motion().await {
        warn!("could not verify AI/motion detection config: {e}");
    }

    // Snapshots for the Home app tiles, served from a background-refreshed
    // cache so requests answer instantly. Secure-video gating: reject when the
    // corresponding snapshot type is disabled.
    let snapshots = SnapshotCache::new(camera.clone());
    snapshots.spawn_refresher();
    let snapshot_hsv = hsv_state.clone();
    let snapshot_host = camera.host.clone();
    let snapshot_name = args.name.clone();
    let snapshot_metrics = metrics.clone();
    let save_snapshots = args.save_snapshots;
    server
        .set_snapshot_handler(Box::new(move |width, height, reason| {
            let snapshots = snapshots.clone();
            let hsv = snapshot_hsv.clone();
            let host = snapshot_host.clone();
            let name = snapshot_name.clone();
            let metrics = snapshot_metrics.clone();
            async move {
                metrics.snapshot_request();
                let periodic_ok = hsv.periodic_snapshots.load(Ordering::SeqCst);
                let event_ok = hsv.event_snapshots.load(Ordering::SeqCst);
                let allowed = match reason {
                    Some(0) => periodic_ok,
                    Some(1) => event_ok,
                    _ => periodic_ok && event_ok,
                };
                if !allowed {
                    let status = match reason {
                        // -70401 insufficient privileges (missing reason),
                        // -70412 not allowed in current state.
                        None => -70401,
                        _ => -70412,
                    };
                    debug!(
                        "snapshot rejected ({width}x{height}, reason {reason:?}) → status {status}"
                    );
                    return Ok(hap::pointer::SnapshotResult::HapStatus(status));
                }
                let result = snapshots.get_scaled(width, height).await;
                match &result {
                    Ok(image) => {
                        metrics.snapshot_success();
                        if save_snapshots {
                            let filename = snapshot_filename(&name);
                            if let Err(e) = tokio::fs::write(&filename, &image.bytes).await {
                                warn!("[{name}@{host}] could not save snapshot to {filename}: {e}");
                            }
                        }
                        debug!(
                            "[{name}@{host}] snapshot served ({width}x{height}, reason {reason:?}, {} bytes, source #{}, age {}ms, source {:016x}, output {:016x}, saved={save_snapshots})",
                            image.bytes.len(),
                            image.source_generation,
                            image.source_age.as_millis(),
                            image.source_fingerprint,
                            image.output_fingerprint,
                        );
                    }
                    Err(e) => {
                        metrics.error(ErrorSubsystem::Snapshot);
                        warn!("[{name}@{host}] snapshot failed ({width}x{height}): {e}");
                    }
                }
                result.map(|image| hap::pointer::SnapshotResult::Jpeg {
                    image: image.bytes,
                    camera_name: name
                        .chars()
                        .map(|c| if c.is_ascii() && !c.is_ascii_control() { c } else { '_' })
                        .collect(),
                    source_generation: image.source_generation,
                    output_fingerprint: image.output_fingerprint,
                })
            }
            .boxed()
        }))
        .await;

    // AI detection events → motion sensors (and the recording trigger flag).
    let (tx, rx) = broadcast::channel::<CameraEvent>(64);
    let event_camera = camera.clone();
    let event_metrics = metrics.clone();
    tokio::spawn(async move { event_camera.run_event_stream(tx, event_metrics).await });
    let mapper = MotionMapper::new(accessory_ptr, motion_active, metrics);
    tokio::spawn(async move { mapper.run(rx).await });

    info!(
        "HomeKit accessory '{}' on port {} — setup code: {}",
        args.name, hap_port, setup_code
    );

    let handle = server.run_handle();
    tokio::select! {
        result = handle => {
            // Exit without unwinding either way: dropping the runtime
            // mid-flight trips a panic in libmdns's task teardown.
            if let Err(e) = result {
                log::error!("server error: {e:?}");
                streams.stop_stream().await;
                std::process::exit(1);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down...");
            streams.stop_stream().await;
            std::process::exit(0);
        }
    }

    Ok(())
}

fn snapshot_filename(camera_name: &str) -> String {
    let basename: String = camera_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{basename}.jpg")
}

/// Finds a free HAP port, preferring the conventional range.
fn find_free_port(host: std::net::IpAddr) -> Result<u16, Box<dyn std::error::Error>> {
    for port in 51826..51926 {
        if std::net::TcpListener::bind((host, port)).is_ok() {
            return Ok(port);
        }
    }
    // Fall back to an ephemeral port.
    let listener = std::net::TcpListener::bind((host, 0))?;
    Ok(listener.local_addr()?.port())
}

/// Small stable content hash (FNV-1a); only used to detect accessory changes.
fn md5_like_hash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

fn parse_pin(pin: &str) -> Result<Pin, Box<dyn std::error::Error>> {
    let digits: Vec<u8> = pin
        .chars()
        .filter(|c| c.is_ascii_digit())
        .map(|c| c as u8 - b'0')
        .collect();
    let digits: [u8; 8] = digits
        .try_into()
        .map_err(|_| format!("PIN must contain exactly 8 digits: {pin}"))?;
    Ok(Pin::new(digits)?)
}

fn generate_pin() -> Pin {
    let mut rng = rand::thread_rng();
    loop {
        let digits = std::array::from_fn(|_| rng.gen_range(0..=9));
        if let Ok(pin) = Pin::new(digits) {
            return pin;
        }
    }
}

fn legacy_default_pin() -> Pin {
    Pin::new([1, 1, 1, 2, 2, 3, 3, 3]).expect("legacy default PIN is valid")
}

fn format_setup_code(pin: &Pin) -> String {
    let digits: String = pin
        .to_string()
        .chars()
        .filter(char::is_ascii_digit)
        .collect();
    let (first, second) = digits.split_at(4);
    format!("{first}-{second}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_pin_accepts_dashed_or_undashed_input_and_displays_four_four() {
        let dashed = parse_pin("1112-2333").unwrap();
        let undashed = parse_pin("11122333").unwrap();

        assert_eq!(dashed, undashed);
        assert_eq!(format_setup_code(&dashed), "1112-2333");
    }
}
