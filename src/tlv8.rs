//! Minimal TLV8 encoder/decoder for the HAP camera streaming payloads.
//!
//! HAP TLV8 items are (type: u8, length: u8, value: [u8; length]). Values longer
//! than 255 bytes are fragmented into consecutive items of the same type.
//! Repeated items of the same type within one container are separated by a
//! zero-length item of type 0x00.

#[derive(Debug, Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn u8(&mut self, tag: u8, value: u8) -> &mut Self {
        self.bytes(tag, &[value])
    }

    pub fn u16(&mut self, tag: u8, value: u16) -> &mut Self {
        self.bytes(tag, &value.to_le_bytes())
    }

    pub fn u32(&mut self, tag: u8, value: u32) -> &mut Self {
        self.bytes(tag, &value.to_le_bytes())
    }

    pub fn bytes(&mut self, tag: u8, value: &[u8]) -> &mut Self {
        if value.is_empty() {
            self.buf.push(tag);
            self.buf.push(0);
            return self;
        }
        for chunk in value.chunks(255) {
            self.buf.push(tag);
            self.buf.push(chunk.len() as u8);
            self.buf.extend_from_slice(chunk);
        }
        self
    }

    /// Writes a zero-length item of type 0x00, used as a delimiter between
    /// repeated items of the same type.
    pub fn delimiter(&mut self) -> &mut Self {
        self.buf.push(0x00);
        self.buf.push(0);
        self
    }

    pub fn build(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buf)
    }
}

/// A parsed TLV8 item.
#[derive(Debug, Clone)]
pub struct Item {
    pub tag: u8,
    pub value: Vec<u8>,
}

/// Parses a TLV8 buffer into items, merging fragmented items (consecutive
/// items of the same type where the previous fragment was 255 bytes long).
pub fn parse(data: &[u8]) -> Vec<Item> {
    let mut items: Vec<Item> = Vec::new();
    let mut i = 0;
    while i + 2 <= data.len() {
        let tag = data[i];
        let len = data[i + 1] as usize;
        let end = (i + 2 + len).min(data.len());
        let value = &data[i + 2..end];

        match items.last_mut() {
            Some(last) if last.tag == tag && last.value.len() % 255 == 0 && !last.value.is_empty() => {
                last.value.extend_from_slice(value);
            }
            _ => items.push(Item {
                tag,
                value: value.to_vec(),
            }),
        }
        i = end;
    }
    items
}

/// Returns the value of the first item with the given tag.
pub fn find(items: &[Item], tag: u8) -> Option<&[u8]> {
    items.iter().find(|i| i.tag == tag).map(|i| i.value.as_slice())
}

pub fn find_u8(items: &[Item], tag: u8) -> Option<u8> {
    find(items, tag).and_then(|v| v.first().copied())
}

pub fn find_u16(items: &[Item], tag: u8) -> Option<u16> {
    find(items, tag).and_then(|v| v.try_into().ok().map(u16::from_le_bytes))
}

pub fn find_u32(items: &[Item], tag: u8) -> Option<u32> {
    find(items, tag).and_then(|v| v.try_into().ok().map(u32::from_le_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_scalars() {
        let buf = Writer::new().u8(0x01, 7).u16(0x02, 0x1234).u32(0x03, 0xdead_beef).build();
        let items = parse(&buf);
        assert_eq!(find_u8(&items, 0x01), Some(7));
        assert_eq!(find_u16(&items, 0x02), Some(0x1234));
        assert_eq!(find_u32(&items, 0x03), Some(0xdead_beef));
    }

    #[test]
    fn fragments_long_values() {
        let long = vec![0xAB; 600];
        let buf = Writer::new().bytes(0x05, &long).build();
        // 600 bytes → fragments of 255 + 255 + 90, each with a 2-byte header.
        assert_eq!(buf.len(), 600 + 3 * 2);
        let items = parse(&buf);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].value, long);
    }

    #[test]
    fn delimiter_separates_repeated_tags() {
        let buf = Writer::new().u8(0x01, 1).delimiter().u8(0x01, 2).build();
        let items = parse(&buf);
        let values: Vec<_> = items.iter().filter(|i| i.tag == 0x01).map(|i| i.value.clone()).collect();
        assert_eq!(values, vec![vec![1], vec![2]]);
    }

    #[test]
    fn parses_nested_containers() {
        let inner = Writer::new().u16(0x03, 1280).u16(0x02, 720).build();
        let buf = Writer::new().bytes(0x01, &inner).build();
        let items = parse(&buf);
        let inner_items = parse(find(&items, 0x01).unwrap());
        assert_eq!(find_u16(&inner_items, 0x03), Some(1280));
        assert_eq!(find_u16(&inner_items, 0x02), Some(720));
    }

    #[test]
    fn empty_value_is_preserved() {
        let buf = Writer::new().bytes(0x09, &[]).build();
        let items = parse(&buf);
        assert_eq!(items.len(), 1);
        assert!(items[0].value.is_empty());
    }
}
