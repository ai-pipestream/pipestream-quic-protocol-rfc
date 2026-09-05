//! Validate the received CBOR representation before applying semantic defaults.

use crate::ProtocolError;
use minicbor::{Decoder, Encoder};

pub(crate) fn fits_f16(value: f32) -> bool {
    let mut bytes = [0; 3];
    Encoder::new(&mut bytes[..])
        .f16(value)
        .expect("fixed buffer");
    Decoder::new(&bytes).f16().expect("encoded f16").to_bits() == value.to_bits()
}

pub(crate) fn validate(bytes: &[u8]) -> Result<(), ProtocolError> {
    let mut position = 0;
    item(bytes, &mut position, 0)?;
    if position != bytes.len() {
        return Err(invalid());
    }
    Ok(())
}

fn invalid() -> ProtocolError {
    ProtocolError::frame("CBOR is malformed or not core deterministic")
}

fn take<'a>(
    bytes: &'a [u8],
    position: &mut usize,
    length: usize,
) -> Result<&'a [u8], ProtocolError> {
    let end = position.checked_add(length).ok_or_else(invalid)?;
    let value = bytes.get(*position..end).ok_or_else(invalid)?;
    *position = end;
    Ok(value)
}

fn item(bytes: &[u8], position: &mut usize, depth: usize) -> Result<(), ProtocolError> {
    if depth > 16 {
        return Err(ProtocolError::limit("CBOR nesting exceeds 16"));
    }
    let start = *position;
    let initial = take(bytes, position, 1)?[0];
    let major = initial >> 5;
    let additional = initial & 31;
    if major == 7 {
        match additional {
            20..=22 => return Ok(()),
            25..=27 => {
                take(bytes, position, 1 << (additional - 24))?;
                let mut decoder = Decoder::new(&bytes[start..*position]);
                let value = decoder.f64().map_err(|_| invalid())?;
                if !value.is_finite()
                    || (additional == 26 && fits_f16(value as f32))
                    || (additional == 27 && f64::from(value as f32).to_bits() == value.to_bits())
                {
                    return Err(invalid());
                }
                return Ok(());
            }
            _ => return Err(invalid()),
        }
    }
    let value = match additional {
        0..=23 => u64::from(additional),
        24..=27 => {
            let width = 1 << (additional - 24);
            let mut value = 0u64;
            for byte in take(bytes, position, width)? {
                value = (value << 8) | u64::from(*byte);
            }
            let minimum = match additional {
                24 => 24,
                25 => 256,
                26 => 65_536,
                _ => 4_294_967_296,
            };
            if value < minimum {
                return Err(invalid());
            }
            value
        }
        _ => return Err(invalid()),
    };
    match major {
        0 | 1 => {}
        2 | 3 => {
            let value = take(
                bytes,
                position,
                usize::try_from(value).map_err(|_| invalid())?,
            )?;
            if major == 3 && std::str::from_utf8(value).is_err() {
                return Err(invalid());
            }
        }
        4 | 5 => {
            // Each array element or map pair needs at least one or two octets.
            if value > ((bytes.len() - *position) / if major == 5 { 2 } else { 1 }) as u64 {
                return Err(invalid());
            }
            let mut previous: Option<&[u8]> = None;
            for _ in 0..value {
                let start = *position;
                item(bytes, position, depth + 1)?;
                if major == 5 {
                    let key = &bytes[start..*position];
                    // RFC 8949 4.2.1: bytewise lexicographic order, not decoded values.
                    if previous.is_some_and(|previous| previous >= key) {
                        return Err(invalid());
                    }
                    previous = Some(key);
                    item(bytes, position, depth + 1)?;
                }
            }
        }
        _ => return Err(invalid()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn independently_specified_representations() {
        for valid in [
            &[0xa0][..],
            &[0xa2, 0x61, b'a', 0, 0x61, b'b', 1],
            &[0xf9, 0x3a, 0x00], // 0.75, RFC 8949 preferred serialization
            &[0xfa, 0x3d, 0xcc, 0xcc, 0xcd], // 0.1f32 needs binary32
        ] {
            validate(valid).unwrap();
        }
        for invalid in [
            &[0xa1, 0x61, b'a', 0x18, 0][..],      // long integer
            &[0xa2, 0x61, b'a', 0, 0x61, b'a', 1], // duplicate key
            &[0xa2, 0x61, b'b', 0, 0x61, b'a', 1], // key order
            &[0xbf, 0xff],                         // indefinite map
            &[0x61, 0xff],                         // invalid UTF-8
            &[0xfa, 0x3f, 0x40, 0, 0],             // 0.75 encoded too wide
            &[0xf9, 0x7c, 0],                      // infinity
            &[0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            &[0, 0], // trailing item
        ] {
            assert!(validate(invalid).is_err(), "{invalid:x?}");
        }
    }
}
