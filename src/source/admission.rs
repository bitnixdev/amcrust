//! Narrow Annex-B admission, not clock repair or an H.264 decoder. We accept one
//! SPS/PPS pair and require every VCL slice to reference it. No cached GOP replay.
use super::Terminal;
use crate::ingest::{Codec, Packet, StreamConfiguration};

pub(super) struct Admission {
    video: usize,
    sample_clock_audio: Option<usize>,
    sps: Vec<u8>,
    pps: Vec<u8>,
    pps_id: u32,
    last_dts: Vec<Option<i64>>,
}

impl Admission {
    pub(super) fn new(streams: &[StreamConfiguration]) -> Result<Self, Terminal> {
        let video = streams
            .iter()
            .find(|s| s.codec == Codec::H264)
            .ok_or(Terminal::Unsupported)?;
        let mut sps = None;
        let mut pps = None;
        for nal in nals(video.parameters.extradata())? {
            match nal[0] & 31 {
                7 if sps.is_none() => sps = Some(nal.to_vec()),
                8 if pps.is_none() => pps = Some(nal.to_vec()),
                _ => return Err(Terminal::Unsupported),
            }
        }
        let (sps, pps) = (
            sps.ok_or(Terminal::Unsupported)?,
            pps.ok_or(Terminal::Unsupported)?,
        );
        let mut sps_bits = Bits::new(&sps[1..])?;
        sps_bits.skip(24)?;
        let sps_id = sps_bits.ue()?;
        let mut pps_bits = Bits::new(&pps[1..])?;
        let pps_id = pps_bits.ue()?;
        if sps_id > 31 || pps_id > 255 || pps_bits.ue()? != sps_id {
            return Err(Terminal::Unsupported);
        }
        Ok(Self {
            video: video.index,
            sample_clock_audio: streams
                .iter()
                .find(|s| {
                    s.codec == Codec::Aac
                        && s.time_base.numerator == 1
                        && s.time_base.denominator == 48000
                        && s.parameters.audio_format().0 == 48000
                        && s.parameters
                            .extradata()
                            .first()
                            .is_some_and(|b| b >> 3 == 2)
                })
                .map(|s| s.index),
            sps,
            pps,
            pps_id,
            last_dts: vec![None; streams.len()],
        })
    }

    pub(super) fn previous_dts(&self, track: usize) -> Option<i64> {
        self.last_dts.get(track).copied().flatten()
    }

    /// None means an initial unclocked packet was deliberately not published.
    /// Known clocks are preserved even when steps differ from packet duration.
    pub(super) fn inspect(
        &mut self,
        packet: &Packet,
        bounded_mapping_step: bool,
    ) -> Result<Option<bool>, Terminal> {
        let track = packet.stream_index();
        let mut idr = false;
        if track == self.video {
            let mut ordinary = false;
            for nal in nals(packet.data())? {
                match nal[0] & 31 {
                    7 if nal == self.sps => (),
                    8 if nal == self.pps => (),
                    7 | 8 => return Err(Terminal::ConfigurationChanged),
                    1 | 5 => {
                        let is_idr = nal[0] & 31 == 5;
                        if is_idr && nal[0] & 0x60 == 0 {
                            return Err(Terminal::Unsupported);
                        }
                        let mut bits = Bits::new(&nal[1..])?;
                        bits.ue()?; // first_mb_in_slice; multiple slices are supported
                        let slice_type = bits.ue()?;
                        if slice_type > 9
                            || (is_idr && !matches!(slice_type % 5, 2 | 4))
                            || bits.ue()? != self.pps_id
                        {
                            return Err(Terminal::Unsupported);
                        }
                        idr |= is_idr;
                        ordinary |= !is_idr;
                    }
                    6 | 9 | 10 | 11 | 12 => (),
                    _ => return Err(Terminal::Unsupported),
                }
            }
            if idr && ordinary {
                return Err(Terminal::Unsupported);
            }
        }
        let (Some(_), Some(dts)) = (packet.pts(), packet.dts()) else {
            // No invented DTS/PTS, including for a key-flagged first video packet.
            // Once this track has a usable clock, losing it retires the generation.
            return if self.last_dts[track].is_none() {
                Ok(None)
            } else {
                Err(Terminal::Clock)
            };
        };
        // Only the audited AAC mapping classifier may authorize this exception.
        // The packet is immutable; strict subscribers are retired before publication.
        if self.last_dts[track].is_some_and(|last| dts <= last)
            && !(bounded_mapping_step && self.sample_clock_audio == Some(track))
        {
            return Err(Terminal::Clock);
        }
        self.last_dts[track] = Some(dts);
        Ok(Some(idr))
    }
}

pub(crate) fn nals(data: &[u8]) -> Result<Vec<&[u8]>, Terminal> {
    let mut units = Vec::new();
    let mut start = None;
    let mut i = 0;
    while i + 3 <= data.len() {
        let prefix = if data[i..].starts_with(&[0, 0, 0, 1]) {
            4
        } else if data[i..].starts_with(&[0, 0, 1]) {
            3
        } else {
            i += 1;
            continue;
        };
        if let Some(begin) = start {
            let mut end = i;
            while end > begin && data[end - 1] == 0 {
                end -= 1;
            }
            if end <= begin {
                return Err(Terminal::Unsupported);
            }
            units.push(&data[begin..end]);
        } else if data[..i].iter().any(|b| *b != 0) {
            return Err(Terminal::Unsupported);
        }
        if units.len() >= 1024 {
            return Err(Terminal::Unsupported);
        }
        start = Some(i + prefix);
        i += prefix;
    }
    let begin = start.ok_or(Terminal::Unsupported)?;
    let mut end = data.len();
    while end > begin && data[end - 1] == 0 {
        end -= 1;
    }
    if end <= begin {
        return Err(Terminal::Unsupported);
    }
    units.push(&data[begin..end]);
    if units.iter().any(|nal| nal[0] & 0x80 != 0) {
        return Err(Terminal::Unsupported);
    }
    Ok(units)
}

// Only parse the small bounded prefix needed for parameter-set identities and
// slice references. Exp-Golomb codes over 31 leading zeros are refused.
struct Bits {
    bytes: Vec<u8>,
    bit: usize,
}
impl Bits {
    fn new(data: &[u8]) -> Result<Self, Terminal> {
        let mut bytes = Vec::with_capacity(128);
        let mut zeros = 0;
        for &byte in data.iter().take(192) {
            if zeros == 2 && byte == 3 {
                zeros = 0;
                continue;
            }
            bytes.push(byte);
            if bytes.len() == 128 {
                break;
            }
            zeros = if byte == 0 { zeros + 1 } else { 0 };
        }
        Ok(Self { bytes, bit: 0 })
    }
    fn next(&mut self) -> Result<u32, Terminal> {
        let byte = *self.bytes.get(self.bit / 8).ok_or(Terminal::Unsupported)?;
        let value = (byte >> (7 - self.bit % 8)) & 1;
        self.bit += 1;
        Ok(value as u32)
    }
    fn skip(&mut self, count: usize) -> Result<(), Terminal> {
        for _ in 0..count {
            self.next()?;
        }
        Ok(())
    }
    fn ue(&mut self) -> Result<u32, Terminal> {
        let mut zeros = 0;
        while self.next()? == 0 {
            zeros += 1;
            if zeros > 31 {
                return Err(Terminal::Unsupported);
            }
        }
        let mut value = 1u32;
        for _ in 0..zeros {
            value = (value << 1) | self.next()?;
        }
        Ok(value - 1)
    }
}
