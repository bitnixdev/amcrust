//! The HomeKit camera accessory: RTP stream management, person/vehicle motion
//! sensors fed by the camera's AI detections, and the HomeKit Secure Video
//! services (operating mode, recording management, data stream transport).

use futures::FutureExt;
use log::{info, warn};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use hap::{
    HapType, Result,
    accessory::{AccessoryInformation, HapAccessory},
    characteristic::{
        AsyncCharacteristicCallbacks, CharacteristicCallbacks, HapCharacteristic, active::ActiveCharacteristic,
    },
    pointer,
    service::{
        HapService, accessory_information::AccessoryInformationService,
        camera_operating_mode::CameraOperatingModeService, camera_recording_management::CameraRecordingManagementService,
        camera_stream_management::CameraStreamManagementService,
        data_stream_transport_management::DataStreamTransportManagementService, motion_sensor::MotionSensorService,
    },
};

use crate::hsv::hds::HdsServer;
use crate::hsv::recording::{self, HsvState};
use crate::stream::{self, StreamManager};
use crate::tlv8;

// Fixed instance-ID bases for each service, spaced to leave room for their
// characteristics.
const IID_STREAM_MGMT: u64 = 20;
const IID_MOTION_PERSON: u64 = 30;
const IID_MOTION_VEHICLE: u64 = 40;
const IID_OPERATING_MODE: u64 = 50;
const IID_RECORDING_MGMT: u64 = 60;
const IID_DATA_STREAM: u64 = 70;

/// MotionSensor service extended with the `Active` characteristic, as the
/// secure-video spec requires.
#[derive(Debug)]
pub struct HsvMotionSensorService {
    inner: MotionSensorService,
    pub active: ActiveCharacteristic,
}

impl HsvMotionSensorService {
    fn new(id: u64, accessory_id: u64) -> Self {
        Self {
            inner: MotionSensorService::new(id, accessory_id),
            active: ActiveCharacteristic::new(id + 8, accessory_id),
        }
    }

    pub fn inner_mut(&mut self) -> &mut MotionSensorService {
        &mut self.inner
    }
}

impl HapService for HsvMotionSensorService {
    fn get_id(&self) -> u64 {
        self.inner.get_id()
    }
    fn set_id(&mut self, id: u64) {
        self.inner.set_id(id)
    }
    fn get_type(&self) -> HapType {
        self.inner.get_type()
    }
    fn set_type(&mut self, hap_type: HapType) {
        self.inner.set_type(hap_type)
    }
    fn get_hidden(&self) -> bool {
        self.inner.get_hidden()
    }
    fn set_hidden(&mut self, hidden: bool) {
        self.inner.set_hidden(hidden)
    }
    fn get_primary(&self) -> bool {
        self.inner.get_primary()
    }
    fn set_primary(&mut self, primary: bool) {
        self.inner.set_primary(primary)
    }
    fn get_linked_services(&self) -> Vec<u64> {
        self.inner.get_linked_services()
    }
    fn set_linked_services(&mut self, linked: Vec<u64>) {
        self.inner.set_linked_services(linked)
    }
    fn get_characteristic(&self, hap_type: HapType) -> Option<&dyn HapCharacteristic> {
        self.get_characteristics().into_iter().find(|c| c.get_type() == hap_type)
    }
    fn get_mut_characteristic(&mut self, hap_type: HapType) -> Option<&mut dyn HapCharacteristic> {
        self.get_mut_characteristics()
            .into_iter()
            .find(|c| c.get_type() == hap_type)
    }
    fn get_characteristics(&self) -> Vec<&dyn HapCharacteristic> {
        let mut characteristics = self.inner.get_characteristics();
        characteristics.push(&self.active);
        characteristics
    }
    fn get_mut_characteristics(&mut self) -> Vec<&mut dyn HapCharacteristic> {
        let mut characteristics = self.inner.get_mut_characteristics();
        characteristics.push(&mut self.active);
        characteristics
    }
}

impl Serialize for HsvMotionSensorService {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("HapService", 5)?;
        state.serialize_field("iid", &self.get_id())?;
        state.serialize_field("type", &self.get_type())?;
        state.serialize_field("hidden", &self.get_hidden())?;
        state.serialize_field("primary", &self.get_primary())?;
        state.serialize_field("characteristics", &self.get_characteristics())?;
        let linked = self.get_linked_services();
        if !linked.is_empty() {
            state.serialize_field("linked", &linked)?;
        }
        state.end()
    }
}

#[derive(Debug)]
pub struct CameraAccessory {
    id: u64,
    pub accessory_information: AccessoryInformationService,
    pub stream_management: CameraStreamManagementService,
    pub motion_person: HsvMotionSensorService,
    pub motion_vehicle: HsvMotionSensorService,
    pub operating_mode: CameraOperatingModeService,
    pub recording_management: CameraRecordingManagementService,
    pub data_stream: DataStreamTransportManagementService,
}

impl CameraAccessory {
    pub async fn new(
        id: u64,
        name: &str,
        model: &str,
        streams: &StreamManager,
        hsv: &Arc<HsvState>,
        hds: &HdsServer,
        secret_slot: pointer::SharedSecretSlot,
    ) -> Result<Self> {
        let accessory_information = AccessoryInformation {
            name: name.into(),
            manufacturer: "Amcrest".into(),
            model: model.into(),
            serial_number: format!("amcrust-{name}"),
            ..Default::default()
        }
        .to_service(1, id)?;

        let stream_management = build_stream_management(id, streams).await?;

        let mut motion_person = HsvMotionSensorService::new(IID_MOTION_PERSON, id);
        if let Some(n) = motion_person.inner_mut().name.as_mut() {
            n.set_value(json!(format!("{name} Person"))).await?;
        }
        // StatusActive=false reads as "sensor not functioning" → the Home app
        // shows the tile as No Response.
        if let Some(s) = motion_person.inner_mut().status_active.as_mut() {
            s.set_value(json!(true)).await?;
        }
        motion_person.active.set_value(json!(1u8)).await?;

        let mut motion_vehicle = HsvMotionSensorService::new(IID_MOTION_VEHICLE, id);
        if let Some(n) = motion_vehicle.inner_mut().name.as_mut() {
            n.set_value(json!(format!("{name} Vehicle"))).await?;
        }
        if let Some(s) = motion_vehicle.inner_mut().status_active.as_mut() {
            s.set_value(json!(true)).await?;
        }
        motion_vehicle.active.set_value(json!(1u8)).await?;

        let operating_mode = build_operating_mode(id, hsv).await?;
        let recording_management = build_recording_management(id, hsv).await?;
        let data_stream = build_data_stream(id, hds, secret_slot).await?;

        Ok(Self {
            id,
            accessory_information,
            stream_management,
            motion_person,
            motion_vehicle,
            operating_mode,
            recording_management,
            data_stream,
        })
    }
}

async fn build_stream_management(id: u64, streams: &StreamManager) -> Result<CameraStreamManagementService> {
    let mut svc = CameraStreamManagementService::new(IID_STREAM_MGMT, id);
    svc.set_primary(true);

    // The secure-video spec requires the Active characteristic on the RTP
    // stream management service (generated at IID_STREAM_MGMT + 7).
    if let Some(active) = svc.active.as_mut() {
        active.set_value(json!(1u8)).await?;
    }

    svc.supported_video_stream_configuration
        .set_value(json!(stream::supported_video_config()))
        .await?;
    svc.supported_audio_stream_configuration
        .set_value(json!(stream::supported_audio_config()))
        .await?;
    svc.supported_rtp_configuration
        .set_value(json!(stream::supported_rtp_config()))
        .await?;

    let streams_ = streams.clone();
    svc.setup_endpoint.on_update_async(Some(move |_old: Vec<u8>, new: Vec<u8>| {
        let streams = streams_.clone();
        async move {
            streams.handle_setup_write(new).await;
            Ok(())
        }
        .boxed()
    }));
    let streams_ = streams.clone();
    svc.setup_endpoint.on_read_async(Some(move || {
        let streams = streams_.clone();
        async move { Ok(streams.setup_read().await) }.boxed()
    }));

    let streams_ = streams.clone();
    svc.selected_stream_configuration
        .on_update_async(Some(move |_old: Vec<u8>, new: Vec<u8>| {
            let streams = streams_.clone();
            async move {
                streams.handle_selected_write(new).await;
                Ok(())
            }
            .boxed()
        }));

    let streams_ = streams.clone();
    svc.streaming_status.on_read_async(Some(move || {
        let streams = streams_.clone();
        async move { Ok(Some(streams.streaming_status().await)) }.boxed()
    }));

    Ok(svc)
}

async fn build_operating_mode(id: u64, hsv: &Arc<HsvState>) -> Result<CameraOperatingModeService> {
    let mut svc = CameraOperatingModeService::new(IID_OPERATING_MODE, id);

    svc.event_snapshots_active
        .set_value(json!(hsv.event_snapshots.load(Ordering::SeqCst)))
        .await?;
    let hsv_ = hsv.clone();
    svc.event_snapshots_active.on_update(Some(move |_: &bool, new: &bool| {
        hsv_.event_snapshots.store(*new, Ordering::SeqCst);
        let hsv = hsv_.clone();
        tokio::spawn(async move { hsv.persist().await });
        Ok(())
    }));

    svc.homekit_camera_active
        .set_value(json!(hsv.homekit_active.load(Ordering::SeqCst)))
        .await?;
    let hsv_ = hsv.clone();
    svc.homekit_camera_active.on_update(Some(move |_: &bool, new: &bool| {
        hsv_.homekit_active.store(*new, Ordering::SeqCst);
        let hsv = hsv_.clone();
        tokio::spawn(async move {
            hsv.persist().await;
            hsv.sync_recorder().await;
        });
        Ok(())
    }));

    if let Some(periodic) = svc.periodic_snapshots_active.as_mut() {
        periodic
            .set_value(json!(hsv.periodic_snapshots.load(Ordering::SeqCst)))
            .await?;
        let hsv_ = hsv.clone();
        periodic.on_update(Some(move |_: &bool, new: &bool| {
            hsv_.periodic_snapshots.store(*new, Ordering::SeqCst);
            let hsv = hsv_.clone();
            tokio::spawn(async move { hsv.persist().await });
            Ok(())
        }));
    }

    if let Some(disabled) = svc.manually_disabled.as_mut() {
        disabled.set_value(json!(false)).await?;
    }
    // Not applicable to these cameras.
    svc.night_vision = None;
    svc.third_party_camera_active = None;
    svc.camera_operating_mode_indicator = None;

    Ok(svc)
}

async fn build_recording_management(id: u64, hsv: &Arc<HsvState>) -> Result<CameraRecordingManagementService> {
    let mut svc = CameraRecordingManagementService::new(IID_RECORDING_MGMT, id);
    svc.set_linked_services(vec![IID_MOTION_PERSON, IID_MOTION_VEHICLE, IID_DATA_STREAM]);

    svc.supported_camera_recording_configuration
        .set_value(json!(recording::supported_camera_recording_config()))
        .await?;
    svc.supported_video_recording_configuration
        .set_value(json!(recording::supported_video_recording_config()))
        .await?;
    svc.supported_audio_recording_configuration
        .set_value(json!(recording::supported_audio_recording_config()))
        .await?;

    svc.active
        .set_value(json!(hsv.recording_active.load(Ordering::SeqCst) as u8))
        .await?;
    let hsv_ = hsv.clone();
    svc.active.on_update_async(Some(move |_old: u8, new: u8| {
        let hsv = hsv_.clone();
        async move {
            info!("recording management active = {new}");
            hsv.recording_active.store(new == 1, Ordering::SeqCst);
            hsv.persist().await;
            hsv.sync_recorder().await;
            Ok(())
        }
        .boxed()
    }));

    if let Some(audio) = svc.recording_audio_active.as_mut() {
        audio
            .set_value(json!(hsv.audio_active.load(Ordering::SeqCst) as u8))
            .await?;
        let hsv_ = hsv.clone();
        audio.on_update_async(Some(move |_old: u8, new: u8| {
            let hsv = hsv_.clone();
            async move {
                hsv.audio_active.store(new == 1, Ordering::SeqCst);
                hsv.persist().await;
                hsv.sync_recorder().await;
                Ok(())
            }
            .boxed()
        }));
    }

    let hsv_ = hsv.clone();
    svc.selected_camera_recording_configuration
        .on_update_async(Some(move |old: Vec<u8>, new: Vec<u8>| {
            let hsv = hsv_.clone();
            async move {
                // Ignore the read-back echo of the value we already hold.
                if new.is_empty() || new == old {
                    return Ok(());
                }
                hsv.handle_selected_write(new).await;
                Ok(())
            }
            .boxed()
        }));
    let hsv_ = hsv.clone();
    svc.selected_camera_recording_configuration.on_read_async(Some(move || {
        let hsv = hsv_.clone();
        async move {
            match hsv.selected_read().await {
                Some(raw) => Ok(Some(raw)),
                None => Err("no selected recording configuration".into()),
            }
        }
        .boxed()
    }));

    Ok(svc)
}

async fn build_data_stream(
    id: u64,
    hds: &HdsServer,
    secret_slot: pointer::SharedSecretSlot,
) -> Result<DataStreamTransportManagementService> {
    let mut svc = DataStreamTransportManagementService::new(IID_DATA_STREAM, id);

    svc.version.set_value(json!("1.0")).await?;
    // Single transport: HomeKit Data Stream over TCP.
    svc.supported_data_stream_transport_configuration
        .set_value(json!(vec![0x01u8, 0x03, 0x01, 0x01, 0x00]))
        .await?;

    // The write handler computes the response; reads return it (full response
    // for the write-response readback, then without the salt afterwards).
    let pending: Arc<std::sync::Mutex<Option<Vec<u8>>>> = Arc::new(std::sync::Mutex::new(None));
    let last: Arc<std::sync::Mutex<Option<Vec<u8>>>> = Arc::new(std::sync::Mutex::new(None));

    let hds_ = hds.clone();
    let pending_ = pending.clone();
    let last_ = last.clone();
    svc.setup_data_stream_transport
        .on_update_async(Some(move |_old: Vec<u8>, new: Vec<u8>| {
            let hds = hds_.clone();
            let pending = pending_.clone();
            let last = last_.clone();
            let secret_slot = secret_slot.clone();
            async move {
                let items = tlv8::parse(&new);
                // A setup request has: command (1 byte), transport type
                // (1 byte), controller key salt (32 bytes). Anything else is
                // our own response echoed back through set_value — ignore it.
                let command = tlv8::find(&items, 0x01);
                let transport = tlv8::find(&items, 0x02);
                let salt = tlv8::find(&items, 0x03);
                let (Some(command), Some(transport), Some(salt)) = (command, transport, salt) else {
                    return Ok(());
                };
                if command.len() != 1 || transport.len() != 1 || salt.len() != 32 {
                    return Ok(());
                }
                if command[0] != 0 || transport[0] != 0 {
                    warn!("unsupported data stream setup: command {command:?} transport {transport:?}");
                    let response = tlv8::Writer::new().u8(0x01, 1).build(); // generic error
                    *pending.lock().unwrap() = Some(response);
                    return Ok(());
                }

                let secret = *secret_slot.read().unwrap();
                let Some(secret) = secret else {
                    warn!("data stream setup without session secret");
                    *pending.lock().unwrap() = Some(tlv8::Writer::new().u8(0x01, 1).build());
                    return Ok(());
                };

                match hds.setup(&secret, salt).await {
                    Ok((port, accessory_salt)) => {
                        info!("data stream session prepared on port {port}");
                        let params = tlv8::Writer::new().u16(0x01, port).build();
                        let full = tlv8::Writer::new()
                            .u8(0x01, 0)
                            .bytes(0x02, &params)
                            .bytes(0x03, &accessory_salt)
                            .build();
                        let without_salt = tlv8::Writer::new().u8(0x01, 0).bytes(0x02, &params).build();
                        *pending.lock().unwrap() = Some(full);
                        *last.lock().unwrap() = Some(without_salt);
                    }
                    Err(e) => {
                        warn!("data stream setup failed: {e}");
                        *pending.lock().unwrap() = Some(tlv8::Writer::new().u8(0x01, 1).build());
                    }
                }
                Ok(())
            }
            .boxed()
        }));

    svc.setup_data_stream_transport.on_read(Some(move || {
        if let Some(full) = pending.lock().unwrap().take() {
            return Ok(Some(full));
        }
        Ok(last.lock().unwrap().clone())
    }));

    Ok(svc)
}

impl HapAccessory for CameraAccessory {
    fn get_id(&self) -> u64 {
        self.id
    }

    fn set_id(&mut self, id: u64) {
        self.id = id;
    }

    fn get_service(&self, hap_type: HapType) -> Option<&dyn HapService> {
        self.get_services().into_iter().find(|s| s.get_type() == hap_type)
    }

    fn get_mut_service(&mut self, hap_type: HapType) -> Option<&mut dyn HapService> {
        self.get_mut_services().into_iter().find(|s| s.get_type() == hap_type)
    }

    fn get_services(&self) -> Vec<&dyn HapService> {
        vec![
            &self.accessory_information,
            &self.stream_management,
            &self.motion_person,
            &self.motion_vehicle,
            &self.operating_mode,
            &self.recording_management,
            &self.data_stream,
        ]
    }

    fn get_mut_services(&mut self) -> Vec<&mut dyn HapService> {
        vec![
            &mut self.accessory_information,
            &mut self.stream_management,
            &mut self.motion_person,
            &mut self.motion_vehicle,
            &mut self.operating_mode,
            &mut self.recording_management,
            &mut self.data_stream,
        ]
    }
}

impl Serialize for CameraAccessory {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("HapAccessory", 2)?;
        state.serialize_field("aid", &self.get_id())?;
        state.serialize_field("services", &self.get_services())?;
        state.end()
    }
}

/// Service IIDs of the two motion sensors, used to address them through the
/// accessory pointer after the accessory has been moved into the server.
pub fn motion_service_iids() -> (u64, u64) {
    (IID_MOTION_PERSON, IID_MOTION_VEHICLE)
}
