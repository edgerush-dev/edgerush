//! What of RFC 6455 the tool speaks: the handshake's key and Accept, and frames written
//! whole and read header first.

/// RFC 6455 §1.3: what a key is hashed with to make the Accept that answers it.
const GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub const TEXT: u8 = 0x1;
pub const CLOSE: u8 = 0x8;

/// Base64 with padding (RFC 4648 §4).
fn base64(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let bytes = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let joined = u32::from(bytes[0]) << 16 | u32::from(bytes[1]) << 8 | u32::from(bytes[2]);
        for at in 0..4 {
            if at <= chunk.len() {
                out.push(ALPHABET[(joined >> (18 - 6 * at) & 0x3f) as usize]);
            } else {
                out.push(b'=');
            }
        }
    }
    out
}

/// A handshake's key: 16 bytes made from `seed`, in base64 (RFC 6455 §4.1).
pub fn key(seed: u64) -> Vec<u8> {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    bytes[8..].copy_from_slice(
        &seed
            .rotate_left(29)
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .to_le_bytes(),
    );
    base64(&bytes)
}

/// The Accept that answers `key` (RFC 6455 §4.2.2).
pub fn accept(key: &[u8]) -> Vec<u8> {
    let mut hashed = key.to_vec();
    hashed.extend_from_slice(GUID);
    base64(&boring::sha::sha1(&hashed))
}

/// Appends a whole frame with `opcode` and `payload`, masked with `mask` if there is one: a
/// client's must be, a server's must not (RFC 6455 §5.1).
pub fn write(out: &mut Vec<u8>, opcode: u8, payload: &[u8], mask: Option<[u8; 4]>) {
    out.push(0x80 | opcode);
    let masked = if mask.is_some() { 0x80 } else { 0 };
    match payload.len() {
        length @ 0..=125 => out.push(masked | length as u8),
        length @ 126..=0xffff => {
            out.push(masked | 126);
            out.extend_from_slice(&(length as u16).to_be_bytes());
        }
        length => {
            out.push(masked | 127);
            out.extend_from_slice(&(length as u64).to_be_bytes());
        }
    }
    match mask {
        Some(mask) => {
            out.extend_from_slice(&mask);
            out.extend(
                payload
                    .iter()
                    .enumerate()
                    .map(|(at, byte)| byte ^ mask[at % 4]),
            );
        }
        None => out.extend_from_slice(payload),
    }
}

/// A frame's header, read from the start of a buffer.
#[derive(Debug, PartialEq, Eq)]
pub struct Header {
    pub opcode: u8,
    pub fin: bool,
    pub mask: Option<[u8; 4]>,
    /// The header's own length: where the payload starts.
    pub length: usize,
    pub payload: usize,
}

impl Header {
    /// The header at the start of `bytes`, if all of it is there.
    pub fn read(bytes: &[u8]) -> Option<Self> {
        let (&first, rest) = bytes.split_first()?;
        let &second = rest.first()?;
        let (payload, mut length) = match second & 0x7f {
            126 => (
                usize::from(u16::from_be_bytes(bytes.get(2..4)?.try_into().ok()?)),
                4,
            ),
            127 => (
                usize::try_from(u64::from_be_bytes(bytes.get(2..10)?.try_into().ok()?)).ok()?,
                10,
            ),
            small => (usize::from(small), 2),
        };
        let mask = if second & 0x80 != 0 {
            let mask = bytes.get(length..length + 4)?.try_into().ok()?;
            length += 4;
            Some(mask)
        } else {
            None
        };
        Some(Self {
            opcode: first & 0x0f,
            fin: first & 0x80 != 0,
            mask,
            length,
            payload,
        })
    }

    /// The frame's whole length, header and payload.
    pub fn whole(&self) -> usize {
        self.length + self.payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_pads_as_rfc_4648_does() {
        assert_eq!(base64(b""), b"");
        assert_eq!(base64(b"f"), b"Zg==");
        assert_eq!(base64(b"fo"), b"Zm8=");
        assert_eq!(base64(b"foo"), b"Zm9v");
        assert_eq!(base64(b"foob"), b"Zm9vYg==");
        assert_eq!(base64(b"fooba"), b"Zm9vYmE=");
        assert_eq!(base64(b"foobar"), b"Zm9vYmFy");
    }

    #[test]
    fn accept_is_rfc_6455s_example() {
        assert_eq!(
            accept(b"dGhlIHNhbXBsZSBub25jZQ=="),
            b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn a_key_is_sixteen_bytes_in_base64() {
        assert_eq!(key(7).len(), 24);
        assert_ne!(key(7), key(8));
    }

    #[test]
    fn frames_read_back_at_every_length_form() {
        for size in [0, 5, 125, 126, 300, 0xffff, 0x10000] {
            for mask in [None, Some([1, 2, 3, 4])] {
                let payload = vec![b'x'; size];
                let mut out = Vec::new();
                write(&mut out, TEXT, &payload, mask);
                let header = Header::read(&out).unwrap();
                assert_eq!(header.opcode, TEXT);
                assert!(header.fin);
                assert_eq!(header.mask, mask);
                assert_eq!(header.payload, size);
                assert_eq!(header.whole(), out.len());
                let unmasked: Vec<u8> = out[header.length..]
                    .iter()
                    .enumerate()
                    .map(|(at, byte)| mask.map_or(*byte, |mask| byte ^ mask[at % 4]))
                    .collect();
                assert_eq!(unmasked, payload);
                // A header cut short is not read.
                assert_eq!(Header::read(&out[..header.length - 1]), None);
            }
        }
    }
}
