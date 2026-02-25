//! The HomeKit Data Stream binary value encoding ("OPACK-like"), as used for
//! HDS header and message dictionaries. See docs/hds-wire-format.md §6.

use std::collections::VecDeque;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Bool(bool),
    Null,
    Int(i64),
    /// Same as Int but always encoded with the 8-byte form (tag 0x33), which
    /// HAP-NodeJS uses for header `id` and `status` fields.
    Int64(i64),
    Float(f64),
    String(String),
    Data(Vec<u8>),
    Array(Vec<Value>),
    Dict(Vec<(String, Value)>),
    Uuid([u8; 16]),
    /// Seconds since 2001-01-01T00:00:00.
    Date(f64),
}

impl Value {
    pub fn dict(entries: Vec<(&str, Value)>) -> Value {
        Value::Dict(entries.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    pub fn get<'a>(&'a self, key: &str) -> Option<&'a Value> {
        match self {
            Value::Dict(entries) => entries.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(v) | Value::Int64(v) => Some(*v),
            Value::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_data(&self) -> Option<&[u8]> {
        match self {
            Value::Data(d) => Some(d),
            _ => None,
        }
    }
}

// --- Encoding ---

pub fn encode(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Bool(true) => out.push(0x01),
        Value::Bool(false) => out.push(0x02),
        Value::Null => out.push(0x04),
        Value::Uuid(bytes) => {
            out.push(0x05);
            out.extend_from_slice(bytes);
        }
        Value::Date(secs) => {
            out.push(0x06);
            out.extend_from_slice(&secs.to_le_bytes());
        }
        Value::Int(v) => encode_int(*v, out),
        Value::Int64(v) => {
            out.push(0x33);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Float(f) => {
            out.push(0x36);
            out.extend_from_slice(&f.to_le_bytes());
        }
        Value::String(s) => {
            let bytes = s.as_bytes();
            match bytes.len() {
                0..=32 => out.push(0x40 + bytes.len() as u8),
                33..=255 => {
                    out.push(0x61);
                    out.push(bytes.len() as u8);
                }
                256..=65535 => {
                    out.push(0x62);
                    out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                }
                _ => {
                    out.push(0x63);
                    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                }
            }
            out.extend_from_slice(bytes);
        }
        Value::Data(d) => {
            // Never use the short form (0x70+len): HAP-NodeJS has a decode bug
            // for it. len8 upwards decodes fine everywhere.
            match d.len() {
                0..=255 => {
                    out.push(0x91);
                    out.push(d.len() as u8);
                }
                256..=65535 => {
                    out.push(0x92);
                    out.extend_from_slice(&(d.len() as u16).to_le_bytes());
                }
                _ => {
                    out.push(0x93);
                    out.extend_from_slice(&(d.len() as u32).to_le_bytes());
                }
            }
            out.extend_from_slice(d);
        }
        Value::Array(items) => {
            if items.len() <= 12 {
                out.push(0xD0 + items.len() as u8);
                for item in items {
                    encode(item, out);
                }
            } else {
                out.push(0xDF);
                for item in items {
                    encode(item, out);
                }
                out.push(0x03);
            }
        }
        Value::Dict(entries) => {
            if entries.len() <= 14 {
                out.push(0xE0 + entries.len() as u8);
                for (k, v) in entries {
                    encode(&Value::String(k.clone()), out);
                    encode(v, out);
                }
            } else {
                out.push(0xEF);
                for (k, v) in entries {
                    encode(&Value::String(k.clone()), out);
                    encode(v, out);
                }
                out.push(0x03);
            }
        }
    }
}

fn encode_int(v: i64, out: &mut Vec<u8>) {
    if v == -1 {
        out.push(0x07);
    } else if (0..=38).contains(&v) {
        // Conservative upper bound: 38 (0x2E). Sources disagree on whether
        // 0x2F is a valid small-int tag, so we never emit it but do accept it.
        out.push(0x08 + v as u8);
    } else if let Ok(v8) = i8::try_from(v) {
        out.push(0x30);
        out.push(v8 as u8);
    } else if let Ok(v16) = i16::try_from(v) {
        out.push(0x31);
        out.extend_from_slice(&v16.to_le_bytes());
    } else if let Ok(v32) = i32::try_from(v) {
        out.push(0x32);
        out.extend_from_slice(&v32.to_le_bytes());
    } else {
        out.push(0x33);
        out.extend_from_slice(&v.to_le_bytes());
    }
}

pub fn encode_to_vec(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode(value, &mut out);
    out
}

// --- Decoding ---

pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
    /// Previously decoded leaf values, for back-reference tags (0xA0–0xCF).
    tracked: VecDeque<Value>,
}

#[derive(Debug)]
pub struct DecodeError(pub String);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HDS decode error: {}", self.0)
    }
}
impl std::error::Error for DecodeError {}

type DecodeResult<T> = Result<T, DecodeError>;

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            tracked: VecDeque::new(),
        }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> DecodeResult<&'a [u8]> {
        if self.remaining() < n {
            return Err(DecodeError(format!("need {n} bytes, have {}", self.remaining())));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn track(&mut self, v: &Value) {
        self.tracked.push_back(v.clone());
    }

    /// Decodes one value. Returns None when a terminator tag (0x03) is read.
    pub fn decode(&mut self) -> DecodeResult<Option<Value>> {
        let tag = self.take(1)?[0];
        let value = match tag {
            0x00 => return Err(DecodeError("zero tag".into())),
            0x01 => Value::Bool(true),
            0x02 => Value::Bool(false),
            0x03 => return Ok(None),
            0x04 => Value::Null,
            0x05 => {
                let mut b = [0u8; 16];
                b.copy_from_slice(self.take(16)?);
                Value::Uuid(b)
            }
            0x06 => Value::Date(f64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            0x07 => Value::Int(-1),
            0x08..=0x2F => Value::Int((tag - 0x08) as i64),
            0x30 => Value::Int(self.take(1)?[0] as i8 as i64),
            0x31 => Value::Int(i16::from_le_bytes(self.take(2)?.try_into().unwrap()) as i64),
            0x32 => Value::Int(i32::from_le_bytes(self.take(4)?.try_into().unwrap()) as i64),
            0x33 => Value::Int(i64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            0x35 => Value::Float(f32::from_le_bytes(self.take(4)?.try_into().unwrap()) as f64),
            0x36 => Value::Float(f64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            0x40..=0x60 => {
                let len = (tag - 0x40) as usize;
                let bytes = self.take(len)?;
                Value::String(String::from_utf8_lossy(bytes).to_string())
            }
            0x61..=0x64 => {
                let len = self.read_length(tag - 0x61 + 1)?;
                let bytes = self.take(len)?;
                Value::String(String::from_utf8_lossy(bytes).to_string())
            }
            0x6F => {
                let start = self.pos;
                while self.take(1)?[0] != 0x00 {}
                Value::String(String::from_utf8_lossy(&self.buf[start..self.pos - 1]).to_string())
            }
            0x70..=0x90 => {
                let len = (tag - 0x70) as usize;
                Value::Data(self.take(len)?.to_vec())
            }
            0x91..=0x94 => {
                let len = self.read_length(tag - 0x91 + 1)?;
                Value::Data(self.take(len)?.to_vec())
            }
            0x9F => {
                let start = self.pos;
                while self.take(1)?[0] != 0x03 {}
                Value::Data(self.buf[start..self.pos - 1].to_vec())
            }
            0xA0..=0xCF => {
                let index = (tag - 0xA0) as usize;
                let v = self
                    .tracked
                    .get(index)
                    .cloned()
                    .ok_or_else(|| DecodeError(format!("back-reference {index} out of range")))?;
                // Back-referenced values are not re-tracked.
                return Ok(Some(v));
            }
            0xD0..=0xDE => {
                let count = (tag - 0xD0) as usize;
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    items.push(self.decode()?.ok_or_else(|| DecodeError("terminator in array".into()))?);
                }
                return Ok(Some(Value::Array(items)));
            }
            0xDF => {
                let mut items = Vec::new();
                while let Some(v) = self.decode()? {
                    items.push(v);
                }
                return Ok(Some(Value::Array(items)));
            }
            0xE0..=0xEE => {
                let count = (tag - 0xE0) as usize;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    let key = self.decode_key()?;
                    let val = self.decode()?.ok_or_else(|| DecodeError("terminator in dict".into()))?;
                    entries.push((key, val));
                }
                return Ok(Some(Value::Dict(entries)));
            }
            0xEF => {
                let mut entries = Vec::new();
                loop {
                    match self.decode()? {
                        None => break,
                        Some(Value::String(key)) => {
                            let val = self.decode()?.ok_or_else(|| DecodeError("terminator in dict".into()))?;
                            entries.push((key, val));
                        }
                        Some(other) => return Err(DecodeError(format!("non-string dict key: {other:?}"))),
                    }
                }
                return Ok(Some(Value::Dict(entries)));
            }
            other => return Err(DecodeError(format!("unknown tag {other:#04x}"))),
        };

        self.track(&value);
        Ok(Some(value))
    }

    fn decode_key(&mut self) -> DecodeResult<String> {
        match self.decode()? {
            Some(Value::String(s)) => Ok(s),
            other => Err(DecodeError(format!("expected string dict key, got {other:?}"))),
        }
    }

    fn read_length(&mut self, width: u8) -> DecodeResult<usize> {
        let bytes = self.take(width as usize)?;
        let mut len = 0usize;
        for (i, b) in bytes.iter().enumerate() {
            len |= (*b as usize) << (8 * i);
        }
        Ok(len)
    }
}

/// Decodes a single top-level value.
pub fn decode_one(buf: &[u8]) -> Result<Value, DecodeError> {
    Decoder::new(buf)
        .decode()?
        .ok_or_else(|| DecodeError("unexpected terminator".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Int64 is an encoding hint only — it decodes as Int.
    fn normalize(v: Value) -> Value {
        match v {
            Value::Int64(i) => Value::Int(i),
            Value::Array(items) => Value::Array(items.into_iter().map(normalize).collect()),
            Value::Dict(entries) => Value::Dict(entries.into_iter().map(|(k, v)| (k, normalize(v))).collect()),
            other => other,
        }
    }

    fn roundtrip(v: Value) {
        let encoded = encode_to_vec(&v);
        let decoded = decode_one(&encoded).unwrap();
        assert_eq!(decoded, normalize(v));
    }

    #[test]
    fn roundtrip_scalars() {
        roundtrip(Value::Bool(true));
        roundtrip(Value::Bool(false));
        roundtrip(Value::Null);
        roundtrip(Value::Int(-1));
        roundtrip(Value::Int(0));
        roundtrip(Value::Int(38));
        roundtrip(Value::Int(39));
        roundtrip(Value::Int(40));
        roundtrip(Value::Int(-42));
        roundtrip(Value::Int(1000));
        roundtrip(Value::Int(100_000));
        roundtrip(Value::Int(10_000_000_000));
        roundtrip(Value::Int64(7));
        roundtrip(Value::Float(1.5));
        roundtrip(Value::String("hello".into()));
        roundtrip(Value::String("x".repeat(100)));
        roundtrip(Value::Data(vec![1, 2, 3]));
        roundtrip(Value::Data(vec![0xAB; 300]));
    }

    #[test]
    fn roundtrip_containers() {
        roundtrip(Value::Array(vec![Value::Int(1), Value::String("a".into())]));
        roundtrip(Value::dict(vec![
            ("protocol", Value::String("control".into())),
            ("request", Value::String("hello".into())),
            ("id", Value::Int64(12345)),
        ]));
        // Large containers use terminated forms.
        roundtrip(Value::Array((0..20).map(Value::Int).collect()));
        let big_dict = Value::Dict((0..20).map(|i| (format!("k{i}"), Value::Int(i))).collect());
        roundtrip(big_dict);
    }

    #[test]
    fn small_int_encoding_matches_spec() {
        assert_eq!(encode_to_vec(&Value::Int(0)), vec![0x08]);
        assert_eq!(encode_to_vec(&Value::Int(38)), vec![0x2E]);
        assert_eq!(encode_to_vec(&Value::Int(-1)), vec![0x07]);
        assert_eq!(encode_to_vec(&Value::Bool(true)), vec![0x01]);
        assert_eq!(encode_to_vec(&Value::String("hi".into())), vec![0x42, b'h', b'i']);
    }

    #[test]
    fn back_references_resolve() {
        // "dataSend" then a back-reference to it (index 0).
        let mut buf = encode_to_vec(&Value::String("dataSend".into()));
        buf.push(0xA0);
        let mut dec = Decoder::new(&buf);
        assert_eq!(dec.decode().unwrap().unwrap(), Value::String("dataSend".into()));
        assert_eq!(dec.decode().unwrap().unwrap(), Value::String("dataSend".into()));
    }

    #[test]
    fn short_form_data_decodes() {
        // We must decode short-form data even though we never emit it.
        let buf = vec![0x73, 1, 2, 3];
        assert_eq!(decode_one(&buf).unwrap(), Value::Data(vec![1, 2, 3]));
    }

    #[test]
    fn hello_response_header_shape() {
        let header = Value::dict(vec![
            ("protocol", Value::String("control".into())),
            ("response", Value::String("hello".into())),
            ("id", Value::Int64(5)),
            ("status", Value::Int64(0)),
        ]);
        let bytes = encode_to_vec(&header);
        // 4-entry dict, id/status must be 9-byte int64 encodings.
        assert_eq!(bytes[0], 0xE4);
        let decoded = decode_one(&bytes).unwrap();
        assert_eq!(decoded.get("id").unwrap().as_i64(), Some(5));
        assert_eq!(decoded.get("status").unwrap().as_i64(), Some(0));
    }
}
