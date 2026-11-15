//! RFC 6184 non-interleaved H.264 packetization (single NAL / FU-A). No RTSP,
//! retransmission, socket, SRTP or RTCP state is shared between viewers.
use super::{Error, rescale};
use crate::{
    ingest::{Codec, TimeBase},
    source::{Delivery, DeliveryGuard, nals},
};
use rand::Rng;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub ssrc: u32,
    pub payload_type: u8,
    /// Whole plaintext RTP datagram, including its 12-byte header. The transport
    /// must reserve SRTP authentication overhead from HomeKit's negotiated MTU.
    pub packet_bytes: usize,
}

pub struct H264 {
    config: Config,
    guard: DeliveryGuard,
    stream: usize,
    time_base: TimeBase,
    origin_pts: i64,
    origin_rtp: u32,
    sequence: u16,
    first_source_sequence: u64,
    last_source_sequence: Option<u64>,
    terminal: Option<Error>,
    packetized: u64,
}
impl H264 {
    pub fn new(first: &Delivery, config: Config) -> Result<Self, Error> {
        if !(96..=127).contains(&config.payload_type)
            || !(64..=65507).contains(&config.packet_bytes)
        {
            return Err(Error::Configuration);
        }
        let packet = first.packet()?;
        let stream = &first.configuration()?[packet.stream_index()];
        if stream.codec != Codec::H264 || !nals(packet.data())?.iter().any(|nal| nal[0] & 31 == 5) {
            return Err(Error::Configuration);
        }
        let mut random = rand::thread_rng();
        Ok(Self {
            config,
            guard: first.guard()?,
            stream: stream.index,
            time_base: stream.time_base,
            origin_pts: packet.pts().ok_or(Error::Clock)?,
            origin_rtp: random.r#gen(),
            sequence: random.r#gen(),
            first_source_sequence: packet.sequence(),
            last_source_sequence: None,
            terminal: None,
            packetized: 0,
        })
    }
    /// Counts constructed datagrams, NOT successful socket writes or decoded frames.
    pub fn packetized(&self) -> u64 {
        self.packetized
    }
    /// The access-unit cursor borrows its charged source delivery. Dropping a
    /// partially emitted AU permanently fails this packetizer rather than pretending
    /// a later P-frame is a continuous stream.
    pub fn begin<'a>(&'a mut self, delivery: &'a Delivery) -> Result<AccessUnit<'a>, Error> {
        let prepared = self.prepare(delivery);
        match prepared {
            Ok((units, timestamp)) => Ok(AccessUnit {
                owner: self,
                delivery,
                units,
                nal: 0,
                offset: 0,
                timestamp,
                complete: false,
            }),
            Err(error) => {
                self.terminal.get_or_insert(error);
                Err(error)
            }
        }
    }
    fn prepare<'a>(&mut self, delivery: &'a Delivery) -> Result<(Vec<&'a [u8]>, u32), Error> {
        if let Some(error) = self.terminal {
            return Err(error);
        }
        self.guard.accepts(delivery)?;
        let packet = delivery.packet()?;
        if packet.stream_index() != self.stream
            || (self.last_source_sequence.is_none()
                && packet.sequence() != self.first_source_sequence)
            || self
                .last_source_sequence
                .is_some_and(|last| packet.sequence() <= last)
        {
            return Err(Error::Order);
        }
        let units = nals(packet.data())?;
        let mut output = Vec::with_capacity(units.len() + 2);
        if units.iter().any(|nal| nal[0] & 31 == 5) {
            // Supply current parameter sets in-band at every IDR. They remain
            // borrowed from the same source-issued configuration, not a stale cache.
            for nal in nals(
                delivery.configuration()?[self.stream]
                    .parameters
                    .extradata(),
            )? {
                if !matches!(nal[0] & 31, 7 | 8) {
                    return Err(Error::Configuration);
                }
                output.push(nal);
            }
        }
        output.extend(units);
        let delta = packet.pts().ok_or(Error::Clock)? as i128 - self.origin_pts as i128;
        let ticks = rescale(
            delta,
            self.time_base,
            TimeBase {
                numerator: 1,
                denominator: 90000,
            },
        )?;
        self.last_source_sequence = Some(packet.sequence());
        Ok((output, self.origin_rtp.wrapping_add(ticks as u32)))
    }
}

pub struct AccessUnit<'a> {
    owner: &'a mut H264,
    delivery: &'a Delivery,
    units: Vec<&'a [u8]>,
    nal: usize,
    offset: usize,
    timestamp: u32,
    complete: bool,
}
impl Drop for AccessUnit<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.owner.terminal.get_or_insert(Error::Incomplete);
        }
    }
}
impl AccessUnit<'_> {
    /// Writes at most one MTU-sized datagram into caller-owned bounded storage.
    /// Rechecks source permission for each fragment. Callers must also fence the
    /// actual transport write; previously copied bytes cannot be recalled.
    pub fn next_packet(&mut self, buffer: &mut [u8]) -> Result<Option<usize>, Error> {
        let result = self.write_next(buffer);
        if let Err(error) = result {
            self.owner.terminal.get_or_insert(error);
        }
        result
    }
    fn write_next(&mut self, buffer: &mut [u8]) -> Result<Option<usize>, Error> {
        if let Some(error) = self.owner.terminal {
            return Err(error);
        }
        self.owner.guard.accepts(self.delivery)?;
        if self.complete {
            return Ok(None);
        }
        let mtu = self.owner.config.packet_bytes;
        if buffer.len() < mtu {
            return Err(Error::Configuration);
        }
        let unit = self.units[self.nal];
        let payload = mtu - 12;
        let size;
        if unit.len() <= payload {
            buffer[12..12 + unit.len()].copy_from_slice(unit);
            size = 12 + unit.len();
            self.nal += 1;
        } else {
            let start = self.offset == 0;
            if start {
                self.offset = 1;
            }
            let amount = (unit.len() - self.offset).min(payload - 2);
            let end = self.offset + amount == unit.len();
            buffer[12] = (unit[0] & 0xe0) | 28;
            buffer[13] = (unit[0] & 31) | if start { 0x80 } else { 0 } | if end { 0x40 } else { 0 };
            buffer[14..14 + amount].copy_from_slice(&unit[self.offset..self.offset + amount]);
            size = 14 + amount;
            self.offset += amount;
            if end {
                self.nal += 1;
                self.offset = 0;
            }
        }
        self.complete = self.nal == self.units.len();
        buffer[0] = 0x80;
        buffer[1] = self.owner.config.payload_type | if self.complete { 0x80 } else { 0 };
        buffer[2..4].copy_from_slice(&self.owner.sequence.to_be_bytes());
        buffer[4..8].copy_from_slice(&self.timestamp.to_be_bytes());
        buffer[8..12].copy_from_slice(&self.owner.config.ssrc.to_be_bytes());
        self.owner.sequence = self.owner.sequence.wrapping_add(1);
        self.owner.packetized = self.owner.packetized.saturating_add(1);
        Ok(Some(size))
    }
}
