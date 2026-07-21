//! Client for a single Amcrest camera: digest-auth HTTP API, snapshots and
//! the `eventManager.cgi` AI event stream.

use chrono::{DateTime, Utc};
use digest_auth::AuthContext;
use log::{debug, error, info, warn};
use md5::{Digest, Md5};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio::time::{Duration, sleep};

/// AI detection event codes to subscribe to.
const EVENT_CODES: &str =
    "SmartMotionHuman,SmartMotionVehicle,CrossLineDetection,CrossRegionDetection";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const RPC_CONFIG_ATTEMPTS: u8 = 3;
const RPC_CONFIG_RETRY_DELAY: Duration = Duration::from_millis(150);

fn config_key_pointer(name: &str, key: &str) -> Option<String> {
    let path = key.strip_prefix(name)?;
    let bytes = path.as_bytes();
    let mut pointer = String::new();
    let mut position = 0;
    while position < bytes.len() {
        match bytes[position] {
            b'.' => {
                position += 1;
                let start = position;
                while position < bytes.len() && !matches!(bytes[position], b'.' | b'[') {
                    position += 1;
                }
                if start == position {
                    return None;
                }
                pointer.push('/');
                pointer.push_str(&path[start..position]);
            }
            b'[' => {
                position += 1;
                let start = position;
                while position < bytes.len() && bytes[position].is_ascii_digit() {
                    position += 1;
                }
                if start == position || bytes.get(position) != Some(&b']') {
                    return None;
                }
                pointer.push('/');
                pointer.push_str(&path[start..position]);
                position += 1;
            }
            _ => return None,
        }
    }
    Some(pointer)
}

fn config_value_like(current: &Value, desired: &str) -> Option<Value> {
    match current {
        Value::Bool(_) => desired.parse::<bool>().ok().map(Value::Bool),
        Value::Number(number) if number.is_i64() => desired
            .parse::<i64>()
            .ok()
            .map(|value| Value::Number(value.into())),
        Value::Number(number) if number.is_u64() => desired
            .parse::<u64>()
            .ok()
            .map(|value| Value::Number(value.into())),
        Value::Number(_) => desired
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number),
        Value::String(_) => Some(Value::String(desired.to_string())),
        _ => None,
    }
}

fn unapplied_rpc_settings(name: &str, table: &Value, desired: &[(String, String)]) -> Vec<String> {
    desired
        .iter()
        .filter_map(|(key, desired_value)| {
            let pointer = config_key_pointer(name, key)?;
            let current = table.pointer(&pointer)?;
            let desired = config_value_like(current, desired_value)?;
            (current != &desired).then(|| key.clone())
        })
        .collect()
}

fn config_name(key: &str) -> Option<&str> {
    let end = key.find(['[', '.']).unwrap_or(key.len());
    (end > 0).then(|| &key[..end])
}

fn settings<const N: usize>(values: [(&str, &str); N]) -> Vec<(String, String)> {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

/// The deterministic media profile applied to every supported camera. Missing
/// model-specific fields are intentionally skipped by the RPC2 config helper.
fn standard_media_profile(ir_lighting: bool) -> Vec<Vec<(String, String)>> {
    let ir_mode = if ir_lighting { "Auto" } else { "Off" };
    let smart_ir = if ir_lighting { "true" } else { "false" };
    let mut profile = vec![
        settings([
            ("AudioInput[0].AudioSource", "Mic"),
            ("AudioInputVolume[0]", "100"),
            ("AudioInDenoise[0].enable", "false"),
            ("AudioInDenoise[0].level", "50"),
        ]),
        settings([
            ("SmartEncode[0].Enable", "false"),
            ("AICoding[0].Enable", "false"),
            ("TwoRefEncode.Enable", "false"),
            ("VideoWaterMark[0].Enable", "false"),
        ]),
        settings([
            ("Encode[0].MainFormat[0].AudioEnable", "true"),
            ("Encode[0].MainFormat[0].Audio.Compression", "AAC"),
            ("Encode[0].MainFormat[0].Audio.Frequency", "48000"),
            ("Encode[0].MainFormat[0].Audio.Bitrate", "64"),
            ("Encode[0].MainFormat[0].Audio.Depth", "16"),
            ("Encode[0].MainFormat[0].Audio.Channels[0]", "0"),
            ("Encode[0].MainFormat[0].Audio.Mode", "0"),
            ("Encode[0].MainFormat[0].Audio.Pack", "DHAV"),
        ]),
        settings([
            ("ImageEnhancement[0].Enable", "false"),
            ("ImageEnhancement[0].CarWindowEnable", "false"),
            ("ImageEnhancement[0].PlateEnable", "false"),
            ("LDCorrection[0].Enable", "false"),
        ]),
        settings([
            ("VideoEncodeROI[0].DynamicTrack", "false"),
            ("VideoEncodeROI[0].Main", "false"),
            ("VideoEncodeROI[0].Extra1", "false"),
            ("VideoEncodeROI[0].Extra2", "false"),
            ("VideoEncodeROI[0].Extra3", "false"),
            ("VideoEncodeROI[0].Snapshot", "false"),
            ("EncodeCrop[0].Extra1.Enable", "false"),
            ("EncodeCrop[0].Extra2.Enable", "false"),
        ]),
        settings([
            ("VideoInMode[0].Mode", "0"),
            ("VideoInMode[0].Config[0]", "0"),
            ("VideoInOptions[0].ExposureMode", "0"),
            ("VideoInOptions[0].ExposureSpeed", "0"),
            ("VideoInOptions[0].GainMin", "0"),
            ("VideoInOptions[0].GainMax", "50"),
            ("VideoInOptions[0].SlowShutter", "false"),
            ("VideoInOptions[0].SmartIRExposure", smart_ir),
        ]),
        settings([
            ("VideoImageControl[0].Flip", "false"),
            ("VideoImageControl[0].Freeze", "false"),
            ("VideoImageControl[0].Mirror", "false"),
            ("VideoImageControl[0].Rotate90", "0"),
            ("VideoImageControl[0].Stable", "0"),
        ]),
    ];

    for config in 0..=2 {
        profile.push(settings([
            (&format!("VideoColor[0][{config}].Brightness"), "50"),
            (&format!("VideoColor[0][{config}].ChromaSuppress"), "50"),
            (&format!("VideoColor[0][{config}].Contrast"), "50"),
            (&format!("VideoColor[0][{config}].Gamma"), "50"),
            (&format!("VideoColor[0][{config}].Hue"), "50"),
            (&format!("VideoColor[0][{config}].Saturation"), "50"),
            (&format!("VideoColor[0][{config}].Style"), "Standard"),
            (
                &format!("VideoColor[0][{config}].TimeSection"),
                "0 00:00:00-24:00:00",
            ),
        ]));
        profile.push(settings([
            (&format!("VideoInBacklight[0][{config}].Mode"), "Off"),
            (&format!("VideoInDayNight[0][{config}].Type"), "Mechanism"),
            (&format!("VideoInDayNight[0][{config}].Mode"), "Brightness"),
            (&format!("VideoInDayNight[0][{config}].Sensitivity"), "2"),
            (&format!("VideoInDayNight[0][{config}].Delay"), "6"),
            (&format!("VideoInDefog[0][{config}].Mode"), "Off"),
            (&format!("VideoInDefog[0][{config}].Intensity"), "0"),
        ]));
        profile.push(settings([
            (&format!("VideoInDenoise[0][{config}].3DType"), "Auto"),
            (
                &format!("VideoInDenoise[0][{config}].3DAutoType.AutoLevel"),
                "50",
            ),
            (
                &format!("VideoInDenoise[0][{config}].3DManulType.SnfLevel"),
                "50",
            ),
            (
                &format!("VideoInDenoise[0][{config}].3DManulType.TnfLevel"),
                "50",
            ),
            (&format!("VideoInSharpness[0][{config}].Sharpness"), "50"),
            (&format!("VideoInWhiteBalance[0][{config}].Mode"), "Auto"),
        ]));
        profile.push(settings([
            (&format!("Lighting[0][{config}].Mode"), ir_mode),
            (&format!("Lighting[0][{config}].Correction"), "50"),
            (&format!("Lighting[0][{config}].Sensitive"), "3"),
            (&format!("Lighting[0][{config}].MiddleLight[0].Angle"), "50"),
            (&format!("Lighting[0][{config}].MiddleLight[0].Light"), "50"),
        ]));
        profile.push(settings([
            (&format!("VideoInRotate[0][{config}].Flip"), "false"),
            (&format!("VideoInRotate[0][{config}].Freeze"), "false"),
            (&format!("VideoInRotate[0][{config}].Mirror"), "false"),
            (&format!("VideoInRotate[0][{config}].Rotate90"), "0"),
            (&format!("VideoInRotate[0][{config}].Stable"), "0"),
        ]));
    }

    profile
}

#[derive(Clone)]
pub struct AmcrestClient {
    pub host: String,
    pub username: String,
    pub password: String,
    client: Client,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VideoProfile {
    pub width: u16,
    pub height: u16,
    pub fps: u8,
    pub bitrate_kbps: u32,
}

impl VideoProfile {
    pub const LIVE_1080P: Self = Self {
        width: 1920,
        height: 1080,
        fps: 15,
        bitrate_kbps: 4096,
    };

    pub const LIVE_720P: Self = Self {
        width: 1280,
        height: 720,
        fps: 15,
        bitrate_kbps: 2048,
    };
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EncoderCapabilities {
    pub main_resolutions: Vec<(u16, u16)>,
    pub extra_resolutions: Vec<(u16, u16)>,
    pub snapshot_resolutions: Vec<(u16, u16)>,
    pub main_bitrate_range: Option<(u32, u32)>,
    pub extra_bitrate_range: Option<(u32, u32)>,
    pub main_fps_max: Option<u8>,
    pub extra_fps_max: Option<u8>,
}

impl EncoderCapabilities {
    pub fn recording_attributes(&self) -> Vec<(u16, u16, u8)> {
        let candidates = [
            (3840, 2160, 15),
            (2688, 1520, 15),
            (1920, 1080, 15),
            (1280, 720, 15),
        ];
        if self.main_resolutions.is_empty() {
            return vec![(1920, 1080, 15), (1280, 720, 15)];
        }
        let supported: Vec<_> = candidates
            .into_iter()
            .filter(|&(width, height, fps)| {
                self.main_resolutions.contains(&(width, height))
                    && self.main_fps_max.is_none_or(|max| fps <= max)
            })
            .collect();
        if supported.is_empty() {
            vec![(1920, 1080, 15), (1280, 720, 15)]
        } else {
            supported
        }
    }

    pub fn best_snapshot_resolution(&self) -> Option<(u16, u16)> {
        self.snapshot_resolutions
            .iter()
            .copied()
            .max_by_key(|&(width, height)| u32::from(width) * u32::from(height))
    }
}

#[derive(Clone, Debug)]
pub struct CameraEvent {
    pub code: String,
    pub action: String,
    pub index: u32,
    pub data: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

impl AmcrestClient {
    pub fn new(host: String, username: String, password: String) -> Self {
        Self {
            host,
            username,
            password,
            client: Client::new(),
        }
    }

    /// RTSP URL for the given stream subtype (0 = main, 1/2 = sub streams).
    pub fn rtsp_url(&self, subtype: u8) -> String {
        format!(
            "rtsp://{}:{}@{}:554/cam/realmonitor?channel=1&subtype={}",
            self.username, self.password, self.host, subtype
        )
    }

    /// Queries the encoder modes exposed by this camera. Firmware families
    /// vary in how completely they populate these fields, so callers still
    /// verify every configuration by reading it back.
    pub async fn encoder_capabilities(
        &self,
    ) -> Result<EncoderCapabilities, Box<dyn std::error::Error + Send + Sync>> {
        let response = match self
            .get("/cgi-bin/encode.cgi?action=getConfigCaps&channel=1")
            .await
        {
            Ok(response) => response,
            Err(channel_one_error) => {
                debug!(
                    "[{}] encode capabilities channel 1 failed ({channel_one_error}); trying channel 0",
                    self.host
                );
                self.get("/cgi-bin/encode.cgi?action=getConfigCaps&channel=0")
                    .await?
            }
        };
        let body = response.text().await?;
        Ok(parse_encoder_capabilities(&body))
    }

    /// Performs a digest-authenticated GET for the given path + query.
    async fn get(
        &self,
        path_and_query: &str,
    ) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("http://{}{}", self.host, path_and_query);

        let resp = self.client.get(&url).send().await?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        if resp.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Err(
                format!("unexpected status {} for {}", resp.status(), path_and_query).into(),
            );
        }

        let www_authenticate = resp
            .headers()
            .get("www-authenticate")
            .ok_or("no WWW-Authenticate header")?
            .to_str()?
            .to_string();

        let context = AuthContext::new(&self.username, &self.password, path_and_query);
        let mut prompt = digest_auth::parse(&www_authenticate)?;
        let auth_header = prompt.respond(&context)?.to_header_string();

        let resp = self
            .client
            .get(&url)
            .header("Authorization", auth_header)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(format!(
                "auth failed with status {} for {}",
                resp.status(),
                path_and_query
            )
            .into());
        }
        Ok(resp)
    }

    /// Returns the camera's model designation, e.g. "IP8M-2796E-AI".
    pub async fn get_device_type(
        &self,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .get("/cgi-bin/magicBox.cgi?action=getDeviceType")
            .await?;
        let body = resp.text().await?;
        // Response format: "type=IP8M-2796E-AI"
        Ok(body
            .trim()
            .strip_prefix("type=")
            .unwrap_or(body.trim())
            .to_string())
    }

    async fn rpc_login(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let challenge = self
            .rpc_post(
                "/RPC2_Login",
                &json!({
                    "method": "global.login",
                    "params": {
                        "userName": self.username,
                        "password": "",
                        "clientType": "Web3.0"
                    },
                    "id": 1
                }),
            )
            .await?;
        let session = challenge
            .get("session")
            .and_then(Value::as_str)
            .ok_or("RPC2 login challenge omitted session")?;
        let authorization = challenge
            .get("params")
            .and_then(|params| params.as_object())
            .ok_or("RPC2 login challenge omitted params")?;
        let realm = authorization
            .get("realm")
            .and_then(Value::as_str)
            .ok_or("RPC2 login challenge omitted realm")?;
        let random = authorization
            .get("random")
            .and_then(Value::as_str)
            .ok_or("RPC2 login challenge omitted random")?;
        let password_hash = format!(
            "{:X}",
            Md5::digest(format!("{}:{realm}:{}", self.username, self.password))
        );
        let login_hash = format!(
            "{:X}",
            Md5::digest(format!("{}:{random}:{password_hash}", self.username))
        );
        let login = self
            .rpc_post(
                "/RPC2_Login",
                &json!({
                    "method": "global.login",
                    "params": {
                        "userName": self.username,
                        "password": login_hash,
                        "clientType": "Web3.0",
                        "authorityType": "Default",
                        "passwordType": "Default"
                    },
                    "id": 2,
                    "session": session
                }),
            )
            .await?;
        Self::require_rpc_success(&login, "global.login")?;
        Ok(session.to_string())
    }

    async fn rpc_post(
        &self,
        endpoint: &str,
        body: &Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .client
            .post(format!("http://{}{}", self.host, endpoint))
            // This is the content type used by the camera's own web client,
            // even though the request body itself is JSON.
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded; charset=UTF-8",
            )
            .body(serde_json::to_vec(body)?)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(format!("RPC2 HTTP status {} for {endpoint}", response.status()).into());
        }
        Ok(serde_json::from_slice(&response.bytes().await?)?)
    }

    fn require_rpc_success(
        response: &Value,
        method: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if response.get("result").and_then(Value::as_bool) == Some(true) {
            return Ok(());
        }
        Err(format!("RPC2 {method} failed: {response}").into())
    }

    async fn rpc_get_config(
        &self,
        session: &str,
        name: &str,
        id: u64,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut last_error = None;
        for attempt in 1..=RPC_CONFIG_ATTEMPTS {
            let result = async {
                let response = self
                    .rpc_post(
                        "/RPC2",
                        &json!({
                            "method": "configManager.getConfig",
                            "params": { "name": name },
                            "id": id,
                            "session": session
                        }),
                    )
                    .await?;
                Self::require_rpc_success(&response, "configManager.getConfig")?;
                response
                    .get("params")
                    .and_then(|params| params.get("table"))
                    .cloned()
                    .ok_or_else(|| format!("RPC2 getConfig {name} omitted params.table").into())
            }
            .await;
            match result {
                Ok(table) => return Ok(table),
                Err(error) => {
                    last_error = Some(error);
                    if attempt < RPC_CONFIG_ATTEMPTS {
                        debug!(
                            "[{}] RPC2 getConfig {name} attempt {attempt} failed; retrying",
                            self.host
                        );
                        sleep(RPC_CONFIG_RETRY_DELAY * u32::from(attempt)).await;
                    }
                }
            }
        }
        Err(last_error.expect("RPC getConfig attempts is nonzero"))
    }

    async fn rpc_set_config(
        &self,
        session: &str,
        name: &str,
        table: &Value,
        id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .rpc_post(
                "/RPC2",
                &json!({
                    "method": "configManager.setConfig",
                    "params": { "name": name, "table": table, "options": [] },
                    "id": id,
                    "session": session
                }),
            )
            .await?;
        Self::require_rpc_success(&response, "configManager.setConfig")
    }

    async fn apply_supported_settings_rpc(
        &self,
        session: &str,
        name: &str,
        desired: &[(String, String)],
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut table = self.rpc_get_config(session, name, 3).await?;
        let mut updates = 0;
        for (key, desired_value) in desired {
            let Some(pointer) = config_key_pointer(name, key) else {
                continue;
            };
            let Some(current) = table.pointer_mut(&pointer) else {
                continue;
            };
            let Some(value) = config_value_like(current, desired_value) else {
                continue;
            };
            if *current != value {
                *current = value;
                updates += 1;
            }
        }
        if updates > 0 {
            self.rpc_set_config(session, name, &table, 4).await?;
            let verified = self.rpc_get_config(session, name, 5).await?;
            let refused = unapplied_rpc_settings(name, &verified, desired);
            if !refused.is_empty() {
                return Err(format!(
                    "camera did not retain {} {name} settings after RPC2 setConfig: {}",
                    refused.len(),
                    refused.join(", ")
                )
                .into());
            }
        }
        Ok(updates)
    }

    pub async fn ensure_recording_encoder(
        &self,
        width: u16,
        height: u16,
        fps: u8,
        gop: u32,
        bitrate_kbps: u32,
        audio_hz: u32,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let desired = [
            ("Encode[0].MainFormat[0].VideoEnable", "true".to_string()),
            (
                "Encode[0].MainFormat[0].Video.Compression",
                "H.264".to_string(),
            ),
            ("Encode[0].MainFormat[0].Video.Width", width.to_string()),
            ("Encode[0].MainFormat[0].Video.Height", height.to_string()),
            ("Encode[0].MainFormat[0].Video.FPS", fps.to_string()),
            ("Encode[0].MainFormat[0].Video.GOP", gop.to_string()),
            (
                "Encode[0].MainFormat[0].Video.BitRate",
                bitrate_kbps.to_string(),
            ),
            (
                "Encode[0].MainFormat[0].Video.BitRateControl",
                "VBR".to_string(),
            ),
            ("Encode[0].MainFormat[0].Video.Profile", "High".to_string()),
            // Amcrest exposes a six-step VBR quality range; use its highest
            // setting while retaining the bitrate selected by HomeKit.
            ("Encode[0].MainFormat[0].Video.Quality", "6".to_string()),
            ("Encode[0].MainFormat[0].Video.Pack", "DHAV".to_string()),
            ("Encode[0].MainFormat[0].Video.Priority", "0".to_string()),
            ("Encode[0].MainFormat[0].Video.SVCTLayer", "1".to_string()),
            ("Encode[0].MainFormat[0].Video.encodeType", "0".to_string()),
            (
                "Encode[0].MainFormat[0].Audio.Compression",
                "AAC".to_string(),
            ),
            (
                "Encode[0].MainFormat[0].Audio.Frequency",
                audio_hz.to_string(),
            ),
            ("Encode[0].MainFormat[0].Audio.Bitrate", "64".to_string()),
            ("Encode[0].MainFormat[0].Audio.Depth", "16".to_string()),
            ("Encode[0].MainFormat[0].Audio.Channels[0]", "0".to_string()),
            ("Encode[0].MainFormat[0].Audio.Mode", "0".to_string()),
            ("Encode[0].MainFormat[0].Audio.Pack", "DHAV".to_string()),
            ("Encode[0].MainFormat[0].AudioEnable", "true".to_string()),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect::<Vec<_>>();
        let session = self.rpc_login().await?;
        self.apply_supported_settings_rpc(&session, "Encode", &desired)
            .await
    }

    fn unapplied_supported_settings(current: &str, desired: &[(String, String)]) -> Vec<String> {
        Self::unapplied_reported_settings(current, current, desired)
    }

    fn unapplied_reported_settings(
        reported: &str,
        actual: &str,
        desired: &[(String, String)],
    ) -> Vec<String> {
        desired
            .iter()
            .filter_map(|(key, value)| {
                let prefix = format!("table.{key}=");
                let expected = format!("table.{key}={value}");
                let supported = reported.lines().any(|line| line.starts_with(&prefix));
                let applied = actual.lines().any(|line| line.trim() == expected);
                (supported && !applied).then(|| key.clone())
            })
            .collect()
    }

    /// Applies the complete detection profile used by HomeKit motion sensors
    /// and recording triggers. Only settings reported by a camera model are
    /// written; the desired values themselves are never inherited defaults.
    pub async fn ensure_smart_motion(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let smart_profile: Vec<(String, String)> = [
            ("SmartMotionDetect[0].Enable", "true"),
            ("SmartMotionDetect[0].ObjectTypes.Human", "true"),
            ("SmartMotionDetect[0].ObjectTypes.Vehicle", "true"),
            ("SmartMotionDetect[0].Sensitivity", "Middle"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
        let mut motion = vec![
            ("MotionDetect[0].Enable".into(), "true".into()),
            ("MotionDetect[0].Level".into(), "3".into()),
            ("MotionDetect[0].EventHandler.Dejitter".into(), "5".into()),
            ("MotionDetect[0].EventHandler.Delay".into(), "0".into()),
            (
                "MotionDetect[0].EventHandler.AlarmOutEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.BeepEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.ExAlarmOutEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.FlashEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.LightingLink.Enable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.LogEnable".into(),
                "true".into(),
            ),
            (
                "MotionDetect[0].EventHandler.MailEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.MatrixEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.MessageEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.PtzLinkEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.RecordEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.SnapshotEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.TipEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.TourEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.VoiceEnable".into(),
                "false".into(),
            ),
            ("MotionDetect[0].OsdTwinkleEnable".into(), "false".into()),
            ("MotionDetect[0].PirMotionLevel".into(), "3".into()),
            ("MotionDetect[0].PtzManualEnable".into(), "false".into()),
        ];
        for day in 0..7 {
            for period in 0..6 {
                motion.push((
                    format!("MotionDetect[0].EventHandler.TimeSection[{day}][{period}]"),
                    if period == 0 {
                        "1 00:00:00-23:59:59".into()
                    } else {
                        "0 00:00:00-23:59:59".into()
                    },
                ));
            }
        }
        for row in 0..18 {
            motion.push((format!("MotionDetect[0].Region[{row}]"), "4194303".into()));
            for window in 0..4 {
                motion.push((
                    format!("MotionDetect[0].MotionDetectWindow[{window}].Region[{row}]"),
                    if window == 0 { "4194303" } else { "0" }.into(),
                ));
            }
        }
        for window in 0..4 {
            motion.push((
                format!("MotionDetect[0].MotionDetectWindow[{window}].Sensitive"),
                "60".into(),
            ));
            motion.push((
                format!("MotionDetect[0].MotionDetectWindow[{window}].Threshold"),
                "5".into(),
            ));
            for coordinate in 0..4 {
                let value = if window == 0 && coordinate >= 2 {
                    "8191"
                } else {
                    "0"
                };
                motion.push((
                    format!("MotionDetect[0].MotionDetectWindow[{window}].Window[{coordinate}]"),
                    value.into(),
                ));
            }
        }
        // SmartMotion is the only analytics engine we use. Explicitly disable
        // any legacy face/IVS rule and its camera-side actions.
        let analyse_profile = [
            ("VideoAnalyseRule[0][0].Enable", "false"),
            ("VideoAnalyseRule[0][0].TrackEnable", "false"),
            ("VideoAnalyseRule[0][0].Config.FeatureEnable", "false"),
            (
                "VideoAnalyseRule[0][0].Config.FeatureExtractEnable",
                "false",
            ),
            (
                "VideoAnalyseRule[0][0].Config.DuplicateRemoval.Enable",
                "false",
            ),
            (
                "VideoAnalyseRule[0][0].Config.FaceBeautification.Enable",
                "false",
            ),
            ("VideoAnalyseRule[0][0].Config.FilterUnAliveEnable", "false"),
            ("VideoAnalyseRule[0][0].Config.snapObjRectEnable", "0"),
            (
                "VideoAnalyseRule[0][0].EventHandler.AlarmOutEnable",
                "false",
            ),
            ("VideoAnalyseRule[0][0].EventHandler.BeepEnable", "false"),
            (
                "VideoAnalyseRule[0][0].EventHandler.ExAlarmOutEnable",
                "false",
            ),
            (
                "VideoAnalyseRule[0][0].EventHandler.LightingLink.Enable",
                "false",
            ),
            ("VideoAnalyseRule[0][0].EventHandler.LogEnable", "false"),
            ("VideoAnalyseRule[0][0].EventHandler.MMSEnable", "false"),
            ("VideoAnalyseRule[0][0].EventHandler.MailEnable", "false"),
            ("VideoAnalyseRule[0][0].EventHandler.MatrixEnable", "false"),
            ("VideoAnalyseRule[0][0].EventHandler.MessageEnable", "false"),
            ("VideoAnalyseRule[0][0].EventHandler.PtzLinkEnable", "false"),
            ("VideoAnalyseRule[0][0].EventHandler.RecordEnable", "false"),
            (
                "VideoAnalyseRule[0][0].EventHandler.SnapshotEnable",
                "false",
            ),
            (
                "VideoAnalyseRule[0][0].EventHandler.SnapshotTitleEnable",
                "false",
            ),
            ("VideoAnalyseRule[0][0].EventHandler.VoiceEnable", "false"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect::<Vec<_>>();
        let session = self.rpc_login().await?;
        let analyse_updates = self
            .apply_supported_settings_rpc(&session, "VideoAnalyseRule", &analyse_profile)
            .await?;

        // Updating the legacy analytics engine resets SmartMotion/MotionDetect
        // on several firmware families. Apply both profiles after
        // VideoAnalyseRule, with MotionDetect deliberately last.
        let final_smart_updates = self
            .apply_supported_settings_rpc(&session, "SmartMotionDetect", &smart_profile)
            .await?;
        let motion_updates = self
            .apply_supported_settings_rpc(&session, "MotionDetect", &motion)
            .await?;
        let total_updates = analyse_updates + final_smart_updates + motion_updates;
        if total_updates > 0 {
            info!(
                "[{}] applied AI/motion normalization ({} settings changed)",
                self.host, total_updates
            );
        }

        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=SmartMotionDetect")
            .await?;
        let verified_smart = resp.text().await?;
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=MotionDetect")
            .await?;
        let verified_motion = resp.text().await?;
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=VideoAnalyseRule")
            .await?;
        let verified_analyse = resp.text().await?;
        let mut refused = Self::unapplied_supported_settings(&verified_analyse, &analyse_profile);
        refused.extend(Self::unapplied_supported_settings(
            &verified_smart,
            &smart_profile,
        ));
        refused.extend(Self::unapplied_supported_settings(
            &verified_motion,
            &motion,
        ));
        if !refused.is_empty() {
            warn!(
                "[{}] camera did not retain {} AI/motion settings after RPC2 setConfig: {}",
                self.host,
                refused.len(),
                refused.join(", ")
            );
        } else {
            debug!("[{}] AI/motion detection profile verified", self.host);
        }
        Ok(())
    }

    /// Applies encoder/config settings via configManager setConfig. `params`
    /// is the raw `Key=Value&Key=Value` query fragment.
    pub async fn set_config(
        &self,
        params: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = format!("/cgi-bin/configManager.cgi?action=setConfig&{params}");
        let resp = self.get(&path).await?;
        let body = resp.text().await?;
        if body.trim() != "OK" {
            return Err(format!("setConfig returned: {}", body.trim()).into());
        }
        Ok(())
    }

    /// Configures the selected live-view substream to its highest verified
    /// packet-copy profile and returns the profile retained by the camera.
    pub async fn ensure_live_substream(
        &self,
        subtype: u8,
        _capabilities: &EncoderCapabilities,
    ) -> Result<VideoProfile, Box<dyn std::error::Error + Send + Sync>> {
        if subtype == 0 {
            return Err("the main stream is reserved for HSV recording".into());
        }
        let idx = subtype - 1;
        let initial = self.encode_config().await?;
        let existing = live_profile_from_config(&initial, idx);
        let candidates: &[VideoProfile] = if subtype == 2 {
            &[VideoProfile::LIVE_1080P, VideoProfile::LIVE_720P]
        } else {
            &[VideoProfile::LIVE_720P]
        };

        for &candidate in candidates {
            if live_config_matches(&initial, idx, candidate) {
                return Ok(candidate);
            }
            info!(
                "[{}] trying live substream {subtype} at {}x{}@{} {}kbps",
                self.host, candidate.width, candidate.height, candidate.fps, candidate.bitrate_kbps,
            );
            if let Err(error) = self.apply_live_profile(idx, candidate).await {
                warn!(
                    "[{}] live substream {subtype} rejected {}x{}: {error}",
                    self.host, candidate.width, candidate.height
                );
                continue;
            }
            let readback = self.encode_config().await?;
            if live_config_matches(&readback, idx, candidate) {
                info!(
                    "[{}] live profile verified: subtype {subtype}, {}x{}@{} {}kbps",
                    self.host,
                    candidate.width,
                    candidate.height,
                    candidate.fps,
                    candidate.bitrate_kbps,
                );
                return Ok(candidate);
            }
            warn!(
                "[{}] camera did not retain {}x{} live profile; trying fallback",
                self.host, candidate.width, candidate.height
            );
        }

        let final_config = self.encode_config().await?;
        live_profile_from_config(&final_config, idx)
            .or(existing)
            .ok_or_else(|| "camera has no usable live substream configuration".into())
    }

    async fn encode_config(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=Encode")
            .await?
            .text()
            .await?)
    }

    async fn apply_live_profile(
        &self,
        idx: u8,
        profile: VideoProfile,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let resolution = format!("{}x{}", profile.width, profile.height);
        let params = format!(
            "Encode%5B0%5D.ExtraFormat%5B{idx}%5D.VideoEnable=true\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Compression=H.264\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.resolution={resolution}\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Width={}\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Height={}\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.FPS={}\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.GOP={}\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.BitRate={}\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.BitRateControl=VBR\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Profile=High\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Quality=6\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Pack=DHAV\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Priority=0\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.SVCTLayer=1\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.AudioEnable=true\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Compression=AAC\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Bitrate=64\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Depth=16\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Channels%5B0%5D=0\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Pack=DHAV\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Frequency=16000",
            profile.width, profile.height, profile.fps, profile.fps, profile.bitrate_kbps,
        );
        self.set_config(&params).await?;
        let body = self.encode_config().await?;
        let ai_gop_key = format!("table.Encode[0].ExtraFormat[{idx}].Video.AiGOP=");
        if body.lines().any(|line| line.starts_with(&ai_gop_key)) {
            self.set_config(&format!(
                "Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.AiGOP={}",
                profile.fps
            ))
            .await?;
        }
        Ok(())
    }

    /// Normalizes every safe, writable media control used by HomeKit. The
    /// active main/live encoder formats are managed separately because their
    /// resolution and bitrate are negotiated at runtime; this profile covers
    /// audio, enhancement features, ROI/crop, exposure, day/night, denoise,
    /// lighting, color, orientation, sharpness, and white balance.
    pub async fn ensure_media_profile(
        &self,
        ir_lighting: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::collections::BTreeMap;

        let mut configs: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
        for setting in standard_media_profile(ir_lighting).into_iter().flatten() {
            let Some(name) = config_name(&setting.0) else {
                continue;
            };
            configs.entry(name.to_string()).or_default().push(setting);
        }

        let session = self.rpc_login().await?;
        let mut updates = 0;
        for (name, desired) in configs {
            updates += self
                .apply_supported_settings_rpc(&session, &name, &desired)
                .await?;
        }
        if updates > 0 {
            info!("[{}] normalized {updates} media settings", self.host);
        }
        Ok(())
    }

    /// Applies a deterministic, minimal burned-in overlay to every camera.
    /// Font sizes are explicit and proportional to each stream's configured
    /// resolution, so the timestamp has the same apparent size after Home
    /// scales a 1080p snapshot or 720p/480p live stream into a tile.
    pub async fn ensure_overlay_profile(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=VideoWidget")
            .await?;
        let body = resp.text().await?;
        let week_position = if body.contains("table.VideoWidget[0].TimeTitle.WeekPosition=Right")
            || body.contains("table.VideoWidget[0].TimeTitle.WeekPosition=Left")
        {
            "Right"
        } else {
            "0"
        };
        let core_settings = [
            ("VideoWidget[0].FontBorder", "true"),
            ("VideoWidget[0].FontSize", "36"),
            ("VideoWidget[0].FontSizeExtra1", "16"),
            ("VideoWidget[0].FontSizeExtra2", "24"),
            ("VideoWidget[0].FontSizeExtra3", "16"),
            ("VideoWidget[0].FontSizeSnapshot", "36"),
            ("VideoWidget[0].FontSizeScale", "1"),
            ("VideoWidget[0].TimeTitle.EncodeBlend", "true"),
            ("VideoWidget[0].TimeTitle.PreviewBlend", "true"),
            ("VideoWidget[0].TimeTitle.ShowWeek", "false"),
            ("VideoWidget[0].TimeTitle.WeekPosition", week_position),
            ("VideoWidget[0].TimeTitle.FrontColor[0]", "255"),
            ("VideoWidget[0].TimeTitle.FrontColor[1]", "255"),
            ("VideoWidget[0].TimeTitle.FrontColor[2]", "255"),
            ("VideoWidget[0].TimeTitle.FrontColor[3]", "0"),
            ("VideoWidget[0].TimeTitle.BackColor[0]", "0"),
            ("VideoWidget[0].TimeTitle.BackColor[1]", "0"),
            ("VideoWidget[0].TimeTitle.BackColor[2]", "0"),
            ("VideoWidget[0].TimeTitle.BackColor[3]", "128"),
            ("VideoWidget[0].TimeTitle.Rect[0]", "5319"),
            ("VideoWidget[0].TimeTitle.Rect[1]", "352"),
            ("VideoWidget[0].TimeTitle.Rect[2]", "7929"),
            ("VideoWidget[0].TimeTitle.Rect[3]", "769"),
            ("VideoWidget[0].ChannelTitle.EncodeBlend", "false"),
            ("VideoWidget[0].ChannelTitle.PreviewBlend", "false"),
            ("VideoWidget[0].OSDMobileState.EncodeBlend", "false"),
            ("VideoWidget[0].OSDMobileState.PreviewBlend", "false"),
            ("VideoWidget[0].PictureTitle.EncodeBlend", "false"),
            ("VideoWidget[0].PictureTitle.PreviewBlend", "false"),
        ];
        let disabled_overlays = [
            "PTZCoordinates",
            "PTZDirection",
            "PTZOSDMenu",
            "PTZOSDMenuViaApp",
            "PTZPreset",
            "PTZZoom",
            "PtzPattern",
            "PtzRS485Detect",
            "Temperature",
            "VoltageStatus",
            "CustomTitle[0]",
            "CustomTitle[1]",
            "CustomTitle[2]",
            "CustomTitle[3]",
            "UserDefinedTitle[0]",
            "UserDefinedTitle[1]",
            "UserDefinedTitle[2]",
            "UserDefinedTitle[3]",
            "Covers[0]",
            "Covers[1]",
            "Covers[2]",
            "Covers[3]",
        ];
        let needs_core_update: Vec<_> = core_settings
            .iter()
            .filter(|(key, value)| {
                let prefix = format!("table.{key}=");
                if !body.lines().any(|line| line.starts_with(&prefix)) {
                    return false;
                }
                if *key == "VideoWidget[0].FontSizeScale" && *value == "1" {
                    return !(body.contains("table.VideoWidget[0].FontSizeScale=1\n")
                        || body.contains("table.VideoWidget[0].FontSizeScale=1\r\n")
                        || body.contains("table.VideoWidget[0].FontSizeScale=1.000000"));
                }
                !body.contains(&format!("table.{key}={value}"))
            })
            .collect();
        if !needs_core_update.is_empty() {
            info!(
                "[{}] normalizing timestamp and overlay appearance",
                self.host
            );
            let params = needs_core_update
                .into_iter()
                .map(|(key, value)| {
                    let encoded = key.replace('[', "%5B").replace(']', "%5D");
                    format!("{encoded}={value}")
                })
                .collect::<Vec<_>>()
                .join("&");
            self.set_config(&params).await?;
        }

        // Optional OSD elements vary by model. Disable only fields this
        // camera reports, using small requests because configManager rejects
        // the complete profile when its URL grows too large.
        for name in disabled_overlays {
            let encode_key = format!("table.VideoWidget[0].{name}.EncodeBlend");
            let preview_key = format!("table.VideoWidget[0].{name}.PreviewBlend");
            let supported = body.contains(&format!("{encode_key}="))
                && body.contains(&format!("{preview_key}="));
            let disabled = body.contains(&format!("{encode_key}=false"))
                && body.contains(&format!("{preview_key}=false"));
            if supported && !disabled {
                let encoded = name.replace('[', "%5B").replace(']', "%5D");
                self.set_config(&format!(
                    "VideoWidget%5B0%5D.{encoded}.EncodeBlend=false\
                     &VideoWidget%5B0%5D.{encoded}.PreviewBlend=false"
                ))
                .await?;
            }
        }
        for suffix in ["Extra1", "Extra2"] {
            let key = format!("table.VideoWidget[0].PTZOSDMenuViaApp.EncodeBlend{suffix}");
            if body.contains(&format!("{key}=")) && !body.contains(&format!("{key}=false")) {
                self.set_config(&format!(
                    "VideoWidget%5B0%5D.PTZOSDMenuViaApp.EncodeBlend{suffix}=false"
                ))
                .await?;
            }
        }
        Ok(())
    }

    /// Configures regular snapshots for maximum JPEG quality. Amcrest firmware
    /// commonly reports snapshot Width/Height fields but treats them as
    /// read-only, so source resolution remains camera-managed.
    pub async fn ensure_snapshot_profile(
        &self,
        capabilities: &EncoderCapabilities,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let desired = vec![
            ("Encode[0].SnapFormat[0].VideoEnable".into(), "true".into()),
            (
                "Encode[0].SnapFormat[0].Video.Compression".into(),
                "MJPG".into(),
            ),
            ("Encode[0].SnapFormat[0].Video.Quality".into(), "6".into()),
        ];
        let session = self.rpc_login().await?;
        self.apply_supported_settings_rpc(&session, "Encode", &desired)
            .await?;
        if let Some((width, height)) = capabilities.best_snapshot_resolution() {
            info!(
                "[{}] snapshot profile verified: MJPEG quality 6, camera-managed resolution (reports up to {}x{})",
                self.host, width, height
            );
        } else {
            info!(
                "[{}] snapshot profile verified: MJPEG quality 6, camera-managed resolution",
                self.host
            );
        }
        Ok(())
    }

    /// Fetches a JPEG snapshot from the camera.
    pub async fn snapshot(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self.get("/cgi-bin/snapshot.cgi").await?;
        Ok(resp.bytes().await?.to_vec())
    }

    /// Runs the AI event stream forever, reconnecting on errors, publishing
    /// events to `tx`.
    pub async fn run_event_stream(
        &self,
        tx: broadcast::Sender<CameraEvent>,
        metrics: std::sync::Arc<crate::metrics::Metrics>,
    ) {
        loop {
            debug!("[{}] connecting to event stream...", self.host);
            match self.stream_events(&tx, &metrics).await {
                Ok(()) => warn!("[{}] event stream ended, reconnecting...", self.host),
                Err(e) => {
                    metrics.error(crate::metrics::ErrorSubsystem::EventStream);
                    error!("[{}] event stream error: {e}, reconnecting...", self.host);
                }
            }
            metrics.event_stream_connected(false);
            metrics.event_stream_reconnect();
            sleep(RECONNECT_DELAY).await;
        }
    }

    async fn stream_events(
        &self,
        tx: &broadcast::Sender<CameraEvent>,
        metrics: &crate::metrics::Metrics,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = format!("/cgi-bin/eventManager.cgi?action=attach&codes=[{EVENT_CODES}]");
        let resp = self.get(&path).await?;

        debug!("[{}] connected, streaming events...", self.host);
        metrics.event_stream_connected(true);

        let mut buffer = String::new();
        let mut stream = resp.bytes_stream();

        use futures::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(newline_pos) = buffer.find('\n') {
                let line = buffer[..newline_pos].trim().to_string();
                buffer = buffer[newline_pos + 1..].to_string();

                if line.starts_with("Code=") {
                    if let Some(event) = parse_event_line(&line) {
                        let _ = tx.send(event);
                    }
                }
            }
        }

        Ok(())
    }
}

/// Serves snapshots from memory, refreshed in the background, so HAP
/// `/resource` requests answer instantly instead of waiting on the camera.
#[derive(Clone)]
pub struct SnapshotCache {
    client: AmcrestClient,
    latest: std::sync::Arc<tokio::sync::RwLock<Option<SnapshotFrame>>>,
    scaled:
        std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<(u32, u32), ScaledSnapshot>>>,
    refresh_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    next_generation: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Clone)]
struct SnapshotFrame {
    generation: u64,
    fetched_at: tokio::time::Instant,
    fingerprint: u64,
    bytes: Vec<u8>,
}

#[derive(Clone)]
struct ScaledSnapshot {
    source_generation: u64,
    source_fetched_at: tokio::time::Instant,
    source_fingerprint: u64,
    bytes: Vec<u8>,
}

pub struct SnapshotImage {
    pub bytes: Vec<u8>,
    pub source_generation: u64,
    pub source_age: Duration,
    pub source_fingerprint: u64,
    pub output_fingerprint: u64,
}

const SNAPSHOT_REFRESH: Duration = Duration::from_secs(10);
const SNAPSHOT_REFRESH_DEDUP: Duration = Duration::from_secs(1);
const SNAPSHOT_FETCH_TIMEOUT: Duration = Duration::from_secs(4);
const SNAPSHOT_SCALE_TIMEOUT: Duration = Duration::from_secs(4);
/// Serve a stale snapshot for up to this long if the camera stops responding.
const SNAPSHOT_MAX_AGE: Duration = Duration::from_secs(120);

impl SnapshotCache {
    pub fn new(client: AmcrestClient) -> Self {
        Self {
            client,
            latest: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
            scaled: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            refresh_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            next_generation: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Spawns the background refresh loop.
    pub fn spawn_refresher(&self) {
        let cache = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SNAPSHOT_REFRESH);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                match cache.refresh().await {
                    Ok(_) => {}
                    Err(e) => warn!("[{}] snapshot refresh failed: {e}", cache.client.host),
                }
            }
        });
    }

    async fn refresh(&self) -> Result<SnapshotFrame, Box<dyn std::error::Error + Send + Sync>> {
        use std::hash::{Hash, Hasher};
        use std::sync::atomic::Ordering;

        let _guard = self.refresh_lock.lock().await;
        if let Some(frame) = self.latest.read().await.clone()
            && frame.fetched_at.elapsed() <= SNAPSHOT_REFRESH_DEDUP
        {
            return Ok(frame);
        }
        let bytes = tokio::time::timeout(SNAPSHOT_FETCH_TIMEOUT, self.client.snapshot())
            .await
            .map_err(|_| {
                format!(
                    "camera snapshot timed out after {}s",
                    SNAPSHOT_FETCH_TIMEOUT.as_secs()
                )
            })??;
        validate_jpeg_envelope(&bytes)?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut hasher);
        let frame = SnapshotFrame {
            generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
            fetched_at: tokio::time::Instant::now(),
            fingerprint: hasher.finish(),
            bytes,
        };
        *self.latest.write().await = Some(frame.clone());
        Ok(frame)
    }

    /// Returns a recent camera frame. A failed refresh may fall back to the
    /// last good frame, but never for longer than SNAPSHOT_MAX_AGE.
    async fn get(&self) -> Result<SnapshotFrame, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(frame) = self.latest.read().await.clone()
            && frame.fetched_at.elapsed() <= SNAPSHOT_MAX_AGE
        {
            return Ok(frame);
        }

        match self.refresh().await {
            Ok(frame) => Ok(frame),
            Err(error) => {
                if let Some(frame) = self.latest.read().await.clone()
                    && frame.fetched_at.elapsed() <= SNAPSHOT_MAX_AGE
                {
                    warn!(
                        "[{}] serving snapshot source aged {:.1}s after refresh failure: {error}",
                        self.client.host,
                        frame.fetched_at.elapsed().as_secs_f32()
                    );
                    return Ok(frame);
                }
                Err(error)
            }
        }
    }

    /// Returns the most recent snapshot scaled to the requested dimensions, as
    /// HAP controllers expect. Scaled variants are cached per size.
    pub async fn get_scaled(
        &self,
        width: u32,
        height: u32,
    ) -> Result<SnapshotImage, Box<dyn std::error::Error + Send + Sync>> {
        let raw = self.get().await?;
        let stale = {
            let scaled = self.scaled.lock().await;
            if let Some(snapshot) = scaled.get(&(width, height))
                && snapshot.source_generation == raw.generation
            {
                return Ok(SnapshotImage {
                    bytes: snapshot.bytes.clone(),
                    source_generation: raw.generation,
                    source_age: raw.fetched_at.elapsed(),
                    source_fingerprint: raw.fingerprint,
                    output_fingerprint: fingerprint(&snapshot.bytes),
                });
            }
            scaled
                .get(&(width, height))
                .filter(|snapshot| snapshot.source_fetched_at.elapsed() <= SNAPSHOT_MAX_AGE)
                .cloned()
        };

        let bytes = match scale_jpeg(raw.bytes, width, height).await {
            Ok(bytes) => bytes,
            Err(error) => {
                if let Some(snapshot) = stale {
                    warn!(
                        "[{}] serving scaled snapshot aged {:.1}s after scaling failure: {error}",
                        self.client.host,
                        snapshot.source_fetched_at.elapsed().as_secs_f32()
                    );
                    return Ok(SnapshotImage {
                        output_fingerprint: fingerprint(&snapshot.bytes),
                        bytes: snapshot.bytes,
                        source_generation: snapshot.source_generation,
                        source_age: snapshot.source_fetched_at.elapsed(),
                        source_fingerprint: snapshot.source_fingerprint,
                    });
                }
                return Err(error);
            }
        };
        self.scaled.lock().await.insert(
            (width, height),
            ScaledSnapshot {
                source_generation: raw.generation,
                source_fetched_at: raw.fetched_at,
                source_fingerprint: raw.fingerprint,
                bytes: bytes.clone(),
            },
        );
        Ok(SnapshotImage {
            output_fingerprint: fingerprint(&bytes),
            bytes,
            source_generation: raw.generation,
            source_age: raw.fetched_at.elapsed(),
            source_fingerprint: raw.fingerprint,
        })
    }
}

/// Scales a JPEG to the given size with ffmpeg, preserving aspect ratio.
async fn scale_jpeg(
    input: Vec<u8>,
    width: u32,
    height: u32,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    tokio::time::timeout(
        SNAPSHOT_SCALE_TIMEOUT,
        scale_jpeg_inner(input, width, height),
    )
    .await
    .map_err(|_| {
        format!(
            "ffmpeg snapshot scaling timed out after {}s",
            SNAPSHOT_SCALE_TIMEOUT.as_secs()
        )
    })?
}

async fn scale_jpeg_inner(
    input: Vec<u8>,
    width: u32,
    height: u32,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    use std::process::Stdio;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Fit within the requested box without distorting the aspect ratio.
    let filter =
        format!("scale='min({width},iw)':'min({height},ih)':force_original_aspect_ratio=decrease");

    // Some Amcrest models occasionally return a valid, completely black JPEG
    // while their snapshot encoder changes buffers. Have FFmpeg identify those
    // frames so the cache can retain the last useful preview instead of making
    // Home briefly replace the tile with black.
    let filter = format!("{filter},blackframe=amount=99:threshold=24");

    let mut child = tokio::process::Command::new("ffmpeg")
        .args(["-hide_banner", "-loglevel", "info"])
        .args(["-f", "image2pipe", "-i", "pipe:0"])
        .args(["-frames:v", "1", "-vf", &filter])
        .args(["-f", "image2", "-c:v", "mjpeg", "-q:v", "4", "pipe:1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let mut stdin = child.stdin.take().ok_or("no stdin")?;
    let mut stdout = child.stdout.take().ok_or("no stdout")?;
    let mut stderr = child.stderr.take().ok_or("no stderr")?;

    let writer = async move {
        let _ = stdin.write_all(&input).await;
        drop(stdin);
    };
    let mut output = Vec::new();
    let reader = stdout.read_to_end(&mut output);
    let mut diagnostics = Vec::new();
    let diagnostics_reader = stderr.read_to_end(&mut diagnostics);

    let (_, read_result, diagnostics_result) = tokio::join!(writer, reader, diagnostics_reader);
    read_result?;
    diagnostics_result?;
    let status = child.wait().await?;
    if !status.success() || output.is_empty() {
        let diagnostics = String::from_utf8_lossy(&diagnostics);
        return Err(format!(
            "ffmpeg snapshot scaling failed (status {status}): {}",
            diagnostics.trim()
        )
        .into());
    }
    if !output.starts_with(&[0xff, 0xd8]) || !output.ends_with(&[0xff, 0xd9]) {
        return Err("ffmpeg snapshot scaling produced an invalid JPEG".into());
    }
    if ffmpeg_detected_black_frame(&diagnostics) {
        return Err("camera produced a nearly all-black snapshot".into());
    }
    Ok(output)
}

fn validate_jpeg_envelope(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if bytes.len() < 4 || !bytes.starts_with(&[0xff, 0xd8]) || !bytes.ends_with(&[0xff, 0xd9]) {
        return Err("camera returned an invalid JPEG snapshot".into());
    }
    Ok(())
}

fn ffmpeg_detected_black_frame(diagnostics: &[u8]) -> bool {
    String::from_utf8_lossy(diagnostics).contains(" pblack:")
}

fn fingerprint(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn parse_encoder_capabilities(body: &str) -> EncoderCapabilities {
    EncoderCapabilities {
        main_resolutions: capability_values(
            body,
            "headMain.Video.ResolutionTypes",
            ".MainFormat[0].Video.ResolutionTypes",
        )
        .as_deref()
        .map(parse_resolutions)
        .unwrap_or_default(),
        extra_resolutions: capability_values(
            body,
            "headExtra.Video.ResolutionTypes",
            ".ExtraFormat[1].Video.ResolutionTypes",
        )
        .as_deref()
        .map(parse_resolutions)
        .unwrap_or_default(),
        snapshot_resolutions: capability_values(
            body,
            "headSnap.Video.ResolutionTypes",
            ".SnapFormat[0].Video.ResolutionTypes",
        )
        .as_deref()
        .map(parse_resolutions)
        .unwrap_or_default(),
        main_bitrate_range: capability_values(
            body,
            "headMain.Video.BitRateOptions",
            ".MainFormat[0].Video.BitRateOptions",
        )
        .as_deref()
        .and_then(parse_bitrate_range),
        extra_bitrate_range: capability_values(
            body,
            "headExtra.Video.BitRateOptions",
            ".ExtraFormat[1].Video.BitRateOptions",
        )
        .as_deref()
        .and_then(parse_bitrate_range),
        main_fps_max: capability_values(
            body,
            "headMain.Video.FPSMax",
            ".MainFormat[0].Video.FPSMax",
        )
        .and_then(|v| v.split(',').next()?.parse().ok()),
        extra_fps_max: capability_values(
            body,
            "headExtra.Video.FPSMax",
            ".ExtraFormat[1].Video.FPSMax",
        )
        .and_then(|v| v.split(',').next()?.parse().ok()),
    }
}

fn capability_values(body: &str, documented: &str, indexed: &str) -> Option<String> {
    let values: Vec<_> = body
        .lines()
        .filter_map(|line| {
            let (key, value) = line.trim().split_once('=')?;
            (key == documented
                || key.starts_with(&format!("{documented}["))
                || key.contains(indexed))
            .then_some(value.trim())
        })
        .collect();
    (!values.is_empty()).then(|| values.join(","))
}

fn live_config_field(body: &str, idx: u8, name: &str) -> Option<String> {
    let prefix = format!("table.Encode[0].ExtraFormat[{idx}].{name}=");
    body.lines().find_map(|line| {
        line.strip_prefix(&prefix)
            .map(|value| value.trim().to_string())
    })
}

fn live_profile_from_config(body: &str, idx: u8) -> Option<VideoProfile> {
    Some(VideoProfile {
        width: live_config_field(body, idx, "Video.Width")?.parse().ok()?,
        height: live_config_field(body, idx, "Video.Height")?.parse().ok()?,
        fps: live_config_field(body, idx, "Video.FPS")?.parse().ok()?,
        bitrate_kbps: live_config_field(body, idx, "Video.BitRate")?
            .parse()
            .ok()?,
    })
}

fn live_config_matches(body: &str, idx: u8, expected: VideoProfile) -> bool {
    live_profile_from_config(body, idx) == Some(expected)
        && live_config_field(body, idx, "VideoEnable").as_deref() == Some("true")
        && live_config_field(body, idx, "Video.Compression").as_deref() == Some("H.264")
        && live_config_field(body, idx, "Video.GOP").and_then(|value| value.parse().ok())
            == Some(u32::from(expected.fps))
        && live_config_field(body, idx, "Video.BitRateControl").as_deref() == Some("VBR")
        && live_config_field(body, idx, "Video.Profile").as_deref() == Some("High")
        && live_config_field(body, idx, "Video.Quality").as_deref() == Some("6")
}

fn parse_bitrate_range(value: &str) -> Option<(u32, u32)> {
    let mut values = value.split(',').filter_map(|part| part.trim().parse().ok());
    Some((values.next()?, values.next()?))
}

fn parse_resolutions(value: &str) -> Vec<(u16, u16)> {
    let mut resolutions = Vec::new();
    for raw in value.split(',') {
        let normalized = raw.trim().to_ascii_uppercase().replace(' ', "");
        let named = match normalized.as_str() {
            "QFHD" | "4K" => Some((3840, 2160)),
            "1080" | "1080P" => Some((1920, 1080)),
            "720" | "720P" => Some((1280, 720)),
            "D1" => Some((704, 480)),
            "VGA" => Some((640, 480)),
            "CIF" => Some((352, 240)),
            _ => None,
        };
        let parsed = named.or_else(|| {
            let mut dimensions = normalized
                .split(|character: char| !character.is_ascii_digit())
                .filter(|part| !part.is_empty())
                .filter_map(|part| part.parse::<u16>().ok());
            Some((dimensions.next()?, dimensions.next()?))
        });
        if let Some(resolution) = parsed
            && !resolutions.contains(&resolution)
        {
            resolutions.push(resolution);
        }
    }
    resolutions
}

fn parse_event_line(line: &str) -> Option<CameraEvent> {
    // Format: Code=SmartMotionHuman;action=Start;index=0;data={...}
    let mut code = None;
    let mut action = None;
    let mut index = 0u32;
    let mut data = serde_json::Value::Null;

    for part in line.splitn(4, ';') {
        if let Some((key, value)) = part.split_once('=') {
            match key.trim() {
                "Code" => code = Some(value.to_string()),
                "action" => action = Some(value.to_string()),
                "index" => index = value.parse().unwrap_or(0),
                "data" => data = serde_json::from_str(value).unwrap_or(serde_json::Value::Null),
                _ => {}
            }
        }
    }

    Some(CameraEvent {
        code: code?,
        action: action?,
        index,
        data,
        timestamp: Utc::now(),
    })
}
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use serde_json::json;

    #[test]
    fn converts_flat_config_keys_to_json_pointers() {
        assert_eq!(config_name("AudioInputVolume[0]"), Some("AudioInputVolume"));
        assert_eq!(config_name("TwoRefEncode.Enable"), Some("TwoRefEncode"));
        assert_eq!(
            config_key_pointer(
                "MotionDetect",
                "MotionDetect[0].EventHandler.TimeSection[6][5]"
            ),
            Some("/0/EventHandler/TimeSection/6/5".to_string())
        );
        assert_eq!(
            config_key_pointer(
                "VideoAnalyseRule",
                "VideoAnalyseRule[0][0].Config.DuplicateRemoval.Enable"
            ),
            Some("/0/0/Config/DuplicateRemoval/Enable".to_string())
        );
        assert_eq!(
            config_key_pointer("MotionDetect", "SmartMotionDetect[0].Enable"),
            None
        );
    }

    #[test]
    fn converts_desired_strings_to_the_existing_json_type() {
        assert_eq!(config_value_like(&json!(true), "false"), Some(json!(false)));
        assert_eq!(config_value_like(&json!(30), "5"), Some(json!(5)));
        assert_eq!(
            config_value_like(&json!("Middle"), "High"),
            Some(json!("High"))
        );
        assert_eq!(config_value_like(&json!([]), "false"), None);
    }

    #[test]
    fn reports_only_supported_rpc_settings_that_still_differ() {
        let table = json!([{"Video": {"FPS": 15, "Profile": "Main"}}]);
        let desired = vec![
            ("Encode[0].Video.FPS".to_string(), "10".to_string()),
            ("Encode[0].Video.Profile".to_string(), "Main".to_string()),
            (
                "Encode[0].Video.Unsupported".to_string(),
                "true".to_string(),
            ),
        ];
        assert_eq!(
            unapplied_rpc_settings("Encode", &table, &desired),
            vec!["Encode[0].Video.FPS"]
        );
    }

    #[test]
    fn standard_media_profile_is_complete_and_has_unique_keys() {
        let profile = standard_media_profile(true);
        let entries: Vec<_> = profile.iter().flatten().collect();
        let keys: HashSet<_> = entries.iter().map(|(key, _)| key).collect();
        assert_eq!(keys.len(), entries.len());

        let expected = [
            ("AudioInDenoise[0].enable", "false"),
            ("SmartEncode[0].Enable", "false"),
            ("VideoInMode[0].Mode", "0"),
            ("VideoInOptions[0].ExposureMode", "0"),
            ("VideoEncodeROI[0].Main", "false"),
            ("VideoInDenoise[0][2].3DType", "Auto"),
            ("Lighting[0][2].Mode", "Auto"),
        ];
        for (key, value) in expected {
            assert!(
                entries
                    .iter()
                    .any(|entry| entry.0 == key && entry.1 == value),
                "missing {key}={value}"
            );
        }
    }

    #[test]
    fn infrared_override_disables_illuminator_in_every_profile() {
        let profile = standard_media_profile(false);
        let entries: Vec<_> = profile.iter().flatten().collect();
        assert!(
            entries.iter().any(|entry| {
                entry.0 == "VideoInOptions[0].SmartIRExposure" && entry.1 == "false"
            })
        );
        for config in 0..=2 {
            let key = format!("Lighting[0][{config}].Mode");
            assert!(
                entries
                    .iter()
                    .any(|entry| entry.0 == key && entry.1 == "Off"),
                "missing {key}=Off"
            );
        }
    }

    #[test]
    fn readback_flags_fields_that_were_reported_but_disappeared() {
        let desired = settings([("VideoInMode[0].Mode", "0")]);
        let refused =
            AmcrestClient::unapplied_reported_settings("table.VideoInMode[0].Mode=2", "", &desired);
        assert_eq!(refused, ["VideoInMode[0].Mode"]);
    }

    #[test]
    fn rejects_malformed_snapshot_envelopes() {
        assert!(validate_jpeg_envelope(&[0xff, 0xd8, 1, 2, 0xff, 0xd9]).is_ok());
        assert!(validate_jpeg_envelope(b"").is_err());
        assert!(validate_jpeg_envelope(b"camera busy").is_err());
        assert!(validate_jpeg_envelope(&[0xff, 0xd8, 1, 2]).is_err());
    }

    #[test]
    fn recognizes_ffmpeg_blackframe_diagnostic() {
        assert!(ffmpeg_detected_black_frame(
            b"[Parsed_blackframe_1] frame:0 pblack:100 pts:0"
        ));
        assert!(!ffmpeg_detected_black_frame(
            b"Input #0, image2pipe, from 'pipe:0'"
        ));
    }

    #[test]
    fn parses_documented_encoder_capabilities_and_orders_best_first() {
        let capabilities = parse_encoder_capabilities(
            "headMain.Video.BitRateOptions=3,8192\n\
             headMain.Video.FPSMax=15\n\
             headMain.Video.ResolutionTypes=720P,1080P,2688x1520,QFHD\n\
             headExtra.Video.BitRateOptions=768,4352\n\
             headExtra.Video.FPSMax=15\n\
             headExtra.Video.ResolutionTypes=720P,1080P\n\
             headSnap.Video.ResolutionTypes=1080P,3840*2160(3840x2160)",
        );
        assert_eq!(capabilities.main_bitrate_range, Some((3, 8192)));
        assert_eq!(capabilities.extra_bitrate_range, Some((768, 4352)));
        assert_eq!(
            capabilities.recording_attributes(),
            vec![
                (3840, 2160, 15),
                (2688, 1520, 15),
                (1920, 1080, 15),
                (1280, 720, 15),
            ]
        );
        assert_eq!(capabilities.best_snapshot_resolution(), Some((3840, 2160)));
    }

    #[test]
    fn parses_indexed_firmware_capability_keys() {
        let capabilities = parse_encoder_capabilities(
            "caps[0].MainFormat[0].Video.ResolutionTypes[0]=3840x2160\n\
             caps[0].MainFormat[0].Video.ResolutionTypes[1]=1920x1080\n\
             caps[0].ExtraFormat[1].Video.ResolutionTypes[0]=1920x1080\n\
             caps[0].ExtraFormat[1].Video.ResolutionTypes[1]=1280x720\n\
             caps[0].SnapFormat[0].Video.ResolutionTypes=3840x2160",
        );
        assert_eq!(capabilities.main_resolutions[0], (3840, 2160));
        assert_eq!(capabilities.main_resolutions[1], (1920, 1080));
        assert_eq!(capabilities.extra_resolutions[0], (1920, 1080));
        assert_eq!(capabilities.snapshot_resolutions[0], (3840, 2160));
    }

    #[test]
    fn verifies_exact_packet_copy_live_profile() {
        let body = "table.Encode[0].ExtraFormat[1].VideoEnable=true\n\
                    table.Encode[0].ExtraFormat[1].Video.Compression=H.264\n\
                    table.Encode[0].ExtraFormat[1].Video.Width=1920\n\
                    table.Encode[0].ExtraFormat[1].Video.Height=1080\n\
                    table.Encode[0].ExtraFormat[1].Video.FPS=15\n\
                    table.Encode[0].ExtraFormat[1].Video.GOP=15\n\
                    table.Encode[0].ExtraFormat[1].Video.BitRate=4096\n\
                    table.Encode[0].ExtraFormat[1].Video.BitRateControl=VBR\n\
                    table.Encode[0].ExtraFormat[1].Video.Profile=High\n\
                    table.Encode[0].ExtraFormat[1].Video.Quality=6";
        assert!(live_config_matches(body, 1, VideoProfile::LIVE_1080P));
        assert!(!live_config_matches(body, 1, VideoProfile::LIVE_720P));
    }
}
