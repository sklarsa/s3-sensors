//! Minimal QWP ingress (QuestDB Wire Protocol over WebSocket) encoder for embedded clients.
//! Spec: <https://questdb.com/docs/connect/wire-protocols/qwp-ingress-websocket/>
//!
//! Covers what a sensor stream needs: several tables per message, fixed-width columns without
//! nulls, and a Gorilla-encoded designated timestamp. No symbols, strings, arrays or decimals.
//! Core-only and self-contained so the tests run on the host: `rustc --edition 2024 --test src/qwp.rs`.

pub const LONG: u8 = 0x05;
pub const FLOAT: u8 = 0x06;
pub const DOUBLE: u8 = 0x07;
const TIMESTAMP: u8 = 0x0A;

const VERSION: u8 = 1;
const FLAG_GORILLA: u8 = 0x04;
const FLAG_DELTA_SYMBOL_DICT: u8 = 0x08;

pub const STATUS_OK: u8 = 0x00;
pub const STATUS_DURABLE_ACK: u8 = 0x02;
pub const STATUS_SCHEMA_MISMATCH: u8 = 0x03;
pub const STATUS_PARSE_ERROR: u8 = 0x05;

/// The output buffer is too small for the message.
#[derive(Debug, PartialEq)]
pub struct Full;

pub struct Encoder<'a> {
    buf: &'a mut [u8],
    len: usize,
    tables: u16,
    bits: u64,
    nbits: u32,
}

impl<'a> Encoder<'a> {
    pub fn new(buf: &'a mut [u8]) -> Result<Self, Full> {
        let mut e = Self {
            buf,
            len: 0,
            tables: 0,
            bits: 0,
            nbits: 0,
        };
        // header; table_count and payload_length are patched in finish()
        e.put(b"QWP1")?;
        e.put(&[
            VERSION,
            FLAG_GORILLA | FLAG_DELTA_SYMBOL_DICT,
            0,
            0,
            0,
            0,
            0,
            0,
        ])?;
        // WebSocket clients always send a delta symbol dictionary; ours is empty (start 0, count 0)
        e.put(&[0, 0])?;
        Ok(e)
    }

    /// Starts a table block. Write each column's values next, in order, then `timestamps`:
    /// the designated timestamp is always the last column.
    pub fn table(&mut self, name: &str, rows: usize, columns: &[(&str, u8)]) -> Result<(), Full> {
        self.tables += 1;
        self.name(name)?;
        self.varint(rows as u64)?;
        self.varint(columns.len() as u64 + 1)?;
        for (n, t) in columns {
            self.name(n)?;
            self.put(&[*t])?;
        }
        self.put(&[0, TIMESTAMP]) // empty name = designated timestamp
    }

    pub fn f32s(&mut self, mut vals: impl Iterator<Item = f32>) -> Result<(), Full> {
        self.put(&[0])?; // null flag: no nulls, no bitmap
        vals.try_for_each(|v| self.put(&v.to_le_bytes()))
    }

    pub fn f64s(&mut self, mut vals: impl Iterator<Item = f64>) -> Result<(), Full> {
        self.put(&[0])?;
        vals.try_for_each(|v| self.put(&v.to_le_bytes()))
    }

    pub fn i64s(&mut self, mut vals: impl Iterator<Item = i64>) -> Result<(), Full> {
        self.put(&[0])?;
        vals.try_for_each(|v| self.put(&v.to_le_bytes()))
    }

    /// Designated timestamps (µs since epoch). Gorilla delta-of-delta: a steady-rate stream costs
    /// ~1 bit per row. Falls back to raw int64s if a delta-of-delta doesn't fit in i32.
    pub fn timestamps(&mut self, mut ts: impl Iterator<Item = i64> + Clone) -> Result<(), Full> {
        self.put(&[0])?; // null flag
        if !dods(ts.clone()).all(|d| i32::try_from(d).is_ok()) {
            self.put(&[0])?; // encoding: uncompressed
            return ts.try_for_each(|t| self.put(&t.to_le_bytes()));
        }
        self.put(&[1])?; // encoding: Gorilla
        for t in ts.clone().take(2) {
            self.put(&t.to_le_bytes())?;
        }
        // prefixes are written as values, LSB-first, like the reference QwpGorillaEncoder
        for d in dods(ts) {
            match d {
                0 => self.bits(0, 1)?,
                -64..=63 => {
                    self.bits(0b01, 2)?;
                    self.bits(d as u64, 7)?
                }
                -256..=255 => {
                    self.bits(0b011, 3)?;
                    self.bits(d as u64, 9)?
                }
                -2048..=2047 => {
                    self.bits(0b0111, 4)?;
                    self.bits(d as u64, 12)?
                }
                _ => {
                    self.bits(0b1111, 4)?;
                    self.bits(d as u64, 32)?
                }
            }
        }
        if self.nbits > 0 {
            self.nbits = 0;
            self.put(&[self.bits as u8])?; // pad to a byte boundary
        }
        Ok(())
    }

    /// Patches the header; returns the message length.
    pub fn finish(self) -> usize {
        self.buf[6..8].copy_from_slice(&self.tables.to_le_bytes());
        self.buf[8..12].copy_from_slice(&((self.len - 12) as u32).to_le_bytes());
        self.len
    }

    fn put(&mut self, b: &[u8]) -> Result<(), Full> {
        let end = self.len + b.len();
        self.buf
            .get_mut(self.len..end)
            .ok_or(Full)?
            .copy_from_slice(b);
        self.len = end;
        Ok(())
    }

    fn varint(&mut self, mut v: u64) -> Result<(), Full> {
        while v >= 0x80 {
            self.put(&[v as u8 | 0x80])?;
            v >>= 7;
        }
        self.put(&[v as u8])
    }

    fn name(&mut self, s: &str) -> Result<(), Full> {
        self.varint(s.len() as u64)?;
        self.put(s.as_bytes())
    }

    fn bits(&mut self, v: u64, n: u32) -> Result<(), Full> {
        if self.nbits == 0 {
            self.bits = 0;
        }
        self.bits |= (v & ((1 << n) - 1)) << self.nbits;
        self.nbits += n;
        while self.nbits >= 8 {
            self.put(&[self.bits as u8])?;
            self.bits >>= 8;
            self.nbits -= 8;
        }
        Ok(())
    }
}

/// Delta-of-deltas from the third timestamp on.
fn dods(mut ts: impl Iterator<Item = i64>) -> impl Iterator<Item = i64> {
    let (a, b) = (ts.next().unwrap_or(0), ts.next().unwrap_or(0));
    let (mut prev, mut delta) = (b, b.wrapping_sub(a));
    ts.map(move |t| {
        let d = t.wrapping_sub(prev);
        let dod = d.wrapping_sub(delta);
        (prev, delta) = (t, d);
        dod
    })
}

pub const WS_BINARY: u8 = 0x2;
pub const WS_CLOSE: u8 = 0x8;
pub const WS_PING: u8 = 0x9;
pub const WS_PONG: u8 = 0xA;

/// `Sec-WebSocket-Key` for the upgrade: base64 of 16 random bytes.
pub fn ws_key(nonce: [u8; 16]) -> [u8; 24] {
    const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = [b'='; 24];
    for (i, c) in nonce.chunks(3).enumerate() {
        let v = (c[0] as u32) << 16
            | (*c.get(1).unwrap_or(&0) as u32) << 8
            | *c.get(2).unwrap_or(&0) as u32;
        for j in 0..=c.len() {
            out[i * 4 + j] = B64[(v >> (18 - 6 * j) & 63) as usize];
        }
    }
    out
}

/// Header for a client-to-server WebSocket frame (clients must mask). Returns its length.
pub fn ws_header(out: &mut [u8; 14], opcode: u8, len: usize, mask: [u8; 4]) -> usize {
    out[0] = 0x80 | opcode; // FIN
    let n = if len < 126 {
        out[1] = 0x80 | len as u8;
        2
    } else if len <= 0xFFFF {
        out[1] = 0x80 | 126;
        out[2..4].copy_from_slice(&(len as u16).to_be_bytes());
        4
    } else {
        out[1] = 0x80 | 127;
        out[2..10].copy_from_slice(&(len as u64).to_be_bytes());
        10
    };
    out[n..n + 4].copy_from_slice(&mask);
    n + 4
}

pub fn ws_mask(payload: &mut [u8], mask: [u8; 4]) {
    for (i, b) in payload.iter_mut().enumerate() {
        *b ^= mask[i & 3];
    }
}

/// A server-to-client (unmasked) frame header: `(opcode, header_len, payload_len)`, or None if
/// more bytes are needed. `b` must hold at least 2 bytes.
pub fn ws_parse_header(b: &[u8]) -> Option<(u8, usize, usize)> {
    let opcode = b[0] & 0x0F;
    match b[1] & 0x7F {
        126 => Some((
            opcode,
            4,
            u16::from_be_bytes(b.get(2..4)?.try_into().ok()?) as usize,
        )),
        127 => Some((
            opcode,
            10,
            u64::from_be_bytes(b.get(2..10)?.try_into().ok()?) as usize,
        )),
        n => Some((opcode, 2, n as usize)),
    }
}

/// Status and sequence of an OK or error response; durable acks have no sequence.
pub fn parse_response(b: &[u8]) -> Option<(u8, Option<i64>, &[u8])> {
    let status = *b.first()?;
    if status == STATUS_DURABLE_ACK {
        return Some((status, None, &[]));
    }
    let seq = i64::from_le_bytes(b.get(1..9)?.try_into().ok()?);
    let msg = if status == STATUS_OK {
        &[][..]
    } else {
        let n = u16::from_le_bytes(b.get(9..11)?.try_into().ok()?) as usize;
        b.get(11..11 + n)?
    };
    Some((status, Some(seq), msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The spec's "single table with three columns" example, up to where it differs from ours:
    /// we always set FLAG_GORILLA (so the timestamp column gets an encoding byte) and send the
    /// empty delta symbol dictionary, like the reference WebSocket client.
    #[test]
    fn spec_example() {
        let mut buf = [0u8; 256];
        let mut e = Encoder::new(&mut buf).unwrap();
        e.table("sensors", 2, &[("id", LONG), ("value", DOUBLE)])
            .unwrap();
        e.i64s([1, 2].into_iter()).unwrap();
        e.f64s([1.3, 2.2].into_iter()).unwrap();
        e.timestamps([10_000_000_000, 400_000].into_iter()).unwrap();
        let n = e.finish();
        #[rustfmt::skip]
        let expected = [
            0x51, 0x57, 0x50, 0x31, 0x01, 0x0C, 0x01, 0x00, (n - 12) as u8, 0, 0, 0, // header
            0x00, 0x00, // empty delta symbol dictionary
            0x07, b's', b'e', b'n', b's', b'o', b'r', b's', 0x02, 0x03,
            0x02, b'i', b'd', 0x05,
            0x05, b'v', b'a', b'l', b'u', b'e', 0x07,
            0x00, 0x0A,
            0x00, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
            0x00, 0xCD, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xF4, 0x3F, 0x9A, 0x99, 0x99, 0x99, 0x99, 0x99, 0x01, 0x40,
            0x00, 0x01, // ts: null flag, Gorilla (two values: both raw)
            0x00, 0xE4, 0x0B, 0x54, 0x02, 0x00, 0x00, 0x00,
            0x80, 0x1A, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(&buf[..n], &expected[..]);
    }

    #[test]
    fn varints() {
        for (v, bytes) in [
            (0u64, &[0x00][..]),
            (127, &[0x7F]),
            (128, &[0x80, 0x01]),
            (300, &[0xAC, 0x02]),
            (16384, &[0x80, 0x80, 0x01]),
        ] {
            let mut buf = [0u8; 32];
            let mut e = Encoder::new(&mut buf).unwrap();
            e.varint(v).unwrap();
            assert_eq!(&e.buf[14..e.len], bytes, "{v}");
        }
    }

    /// Decodes our Gorilla stream with the spec's bucket table to check the bit layout.
    #[test]
    fn gorilla_roundtrip() {
        let ts: [i64; 9] = [
            1_000_000, 1_000_182, 1_000_365, 1_000_547, 1_000_547, 1_000_850, 1_003_000, 1_100_000,
            1_100_182,
        ];
        let mut buf = [0u8; 128];
        let mut e = Encoder::new(&mut buf).unwrap();
        e.timestamps(ts.iter().copied()).unwrap();
        let n = e.len;
        let b = &buf[14..n];
        assert_eq!(&b[..2], &[0, 1]); // null flag, Gorilla
        let first = i64::from_le_bytes(b[2..10].try_into().unwrap());
        let second = i64::from_le_bytes(b[10..18].try_into().unwrap());
        let bits = &b[18..];
        let mut pos = 0;
        let mut read = |n: u32| -> u64 {
            let mut v = 0;
            for i in 0..n {
                v |= (((bits[pos / 8] >> (pos % 8)) & 1) as u64) << i;
                pos += 1;
            }
            v
        };
        let signed = |v: u64, n: u32| ((v << (64 - n)) as i64) >> (64 - n);
        let (mut out, mut delta) = (vec![first, second], second - first);
        for _ in 2..ts.len() {
            let dod = if read(1) == 0 {
                0
            } else if read(1) == 0 {
                signed(read(7), 7)
            } else if read(1) == 0 {
                signed(read(9), 9)
            } else if read(1) == 0 {
                signed(read(12), 12)
            } else {
                signed(read(32), 32)
            };
            delta += dod;
            out.push(out.last().unwrap() + delta);
        }
        assert_eq!(out, ts);
    }

    #[test]
    fn gorilla_falls_back_when_dod_overflows_i32() {
        let mut buf = [0u8; 64];
        let mut e = Encoder::new(&mut buf).unwrap();
        e.timestamps([0, 1, 1 << 40].into_iter()).unwrap();
        assert_eq!(&e.buf[14..16], &[0, 0]); // null flag, uncompressed
        assert_eq!(e.len, 16 + 3 * 8);
    }

    #[test]
    fn full_buffer() {
        let mut buf = [0u8; 20];
        let mut e = Encoder::new(&mut buf).unwrap();
        assert_eq!(e.f64s([1.0].into_iter()), Err(Full));
    }

    #[test]
    fn ws_frames() {
        let mut h = [0u8; 14];
        assert_eq!(&ws_key(*b"the sample nonce"), b"dGhlIHNhbXBsZSBub25jZQ=="); // RFC 6455 §1.3
        assert_eq!(ws_header(&mut h, WS_BINARY, 5, [1, 2, 3, 4]), 6);
        assert_eq!(&h[..6], &[0x82, 0x85, 1, 2, 3, 4]);
        assert_eq!(ws_header(&mut h, WS_BINARY, 300, [0; 4]), 8);
        assert_eq!(&h[..4], &[0x82, 0xFE, 0x01, 0x2C]);
        assert_eq!(ws_header(&mut h, WS_BINARY, 70_000, [0; 4]), 14);
        assert_eq!(&h[..10], &[0x82, 0xFF, 0, 0, 0, 0, 0, 0x01, 0x11, 0x70]);
        let mut p = *b"abcde";
        ws_mask(&mut p, [1, 2, 3, 4]);
        assert_eq!(p, [b'a' ^ 1, b'b' ^ 2, b'c' ^ 3, b'd' ^ 4, b'e' ^ 1]);
        assert_eq!(ws_parse_header(&[0x82, 0x05]), Some((2, 2, 5)));
        assert_eq!(
            ws_parse_header(&[0x82, 0x7E, 0x01, 0x2C]),
            Some((2, 4, 300))
        );
        assert_eq!(ws_parse_header(&[0x82, 0x7E]), None);
    }

    #[test]
    fn responses() {
        let ok = [0u8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(parse_response(&ok), Some((STATUS_OK, Some(7), &[][..])));
        let err = [5u8, 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, b'n', b'o'];
        assert_eq!(
            parse_response(&err),
            Some((STATUS_PARSE_ERROR, Some(3), &b"no"[..]))
        );
        assert_eq!(
            parse_response(&[2, 0, 0]),
            Some((STATUS_DURABLE_ACK, None, &[][..]))
        );
    }
}
