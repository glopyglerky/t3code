//! Compact live positional lists. Delta/run varints avoid JSON's repeated
//! absolute offsets; base64 keeps omdb's existing JSON witnesses self-contained.

use super::{Positions, validate_positions};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let word = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        encoded.push(ALPHABET[(word >> 18) as usize] as char);
        encoded.push(ALPHABET[((word >> 12) & 63) as usize] as char);
        encoded.push(if chunk.len() > 1 {
            ALPHABET[((word >> 6) & 63) as usize] as char
        } else {
            '='
        });
        encoded.push(if chunk.len() > 2 {
            ALPHABET[(word & 63) as usize] as char
        } else {
            '='
        });
    }
    encoded
}

fn unbase64(encoded: &str) -> Result<Vec<u8>, &'static str> {
    if !encoded.len().is_multiple_of(4) {
        return Err("invalid position base64 length");
    }
    let mut result = Vec::with_capacity(encoded.len() / 4 * 3);
    for (index, chunk) in encoded.as_bytes().chunks(4).enumerate() {
        let digit = |byte| match byte {
            b'A'..=b'Z' => Ok(u32::from(byte - b'A')),
            b'a'..=b'z' => Ok(u32::from(byte - b'a') + 26),
            b'0'..=b'9' => Ok(u32::from(byte - b'0') + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err("invalid position base64 digit"),
        };
        let a = digit(chunk[0])?;
        let b = digit(chunk[1])?;
        let c = if chunk[2] == b'=' {
            0
        } else {
            digit(chunk[2])?
        };
        let d = if chunk[3] == b'=' {
            0
        } else {
            digit(chunk[3])?
        };
        if (chunk[2] == b'=' && chunk[3] != b'=')
            || (chunk.contains(&b'=') && (index + 1) * 4 != encoded.len())
        {
            return Err("invalid position base64 padding");
        }
        let word = (a << 18) | (b << 12) | (c << 6) | d;
        result.push((word >> 16) as u8);
        if chunk[2] != b'=' {
            result.push((word >> 8) as u8);
        }
        if chunk[3] != b'=' {
            result.push(word as u8);
        }
    }
    if base64(&result) != encoded {
        return Err("noncanonical position base64");
    }
    Ok(result)
}

fn put_varint(bytes: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        bytes.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    bytes.push(value as u8);
}

fn get_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, &'static str> {
    let mut result = 0;
    for shift in (0..=63).step_by(7) {
        let byte = *bytes.get(*cursor).ok_or("truncated position varint")?;
        *cursor += 1;
        if shift == 63 && byte > 1 {
            return Err("overflowing position varint");
        }
        result |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            if shift > 0 && byte == 0 {
                return Err("noncanonical position varint");
            }
            return Ok(result);
        }
    }
    Err("overflowing position varint")
}

fn pack(positions: &Positions) -> Result<String, &'static str> {
    validate_positions(positions).map_err(|_| "invalid positional ranges")?;
    let mut bytes = Vec::new();
    let mut previous: Option<u64> = None;
    for &[start, end] in positions {
        let base = match previous {
            Some(end) => end.checked_add(1).ok_or("position overflow")?,
            None => 0,
        };
        let gap = start.checked_sub(base).ok_or("position order")?;
        let control = gap
            .checked_mul(2)
            .and_then(|gap| gap.checked_add(u64::from(end != start)))
            .ok_or("position overflow")?;
        put_varint(&mut bytes, control);
        if end != start {
            put_varint(&mut bytes, end - start);
        }
        previous = Some(end);
    }
    Ok(format!("p1:{}", base64(&bytes)))
}

fn unpack(encoded: &str) -> Result<Positions, &'static str> {
    let bytes = unbase64(
        encoded
            .strip_prefix("p1:")
            .ok_or("unknown position codec")?,
    )?;
    let mut cursor = 0;
    let mut result: Positions = Vec::new();
    while cursor < bytes.len() {
        let control = get_varint(&bytes, &mut cursor)?;
        let gap = control >> 1;
        let base = match result.last() {
            Some([_, end]) => {
                if gap == 0 {
                    return Err("adjacent positional ranges must be combined");
                }
                end.checked_add(1).ok_or("position overflow")?
            }
            None => 0,
        };
        let start = base.checked_add(gap).ok_or("position overflow")?;
        let span = if control & 1 != 0 {
            let span = get_varint(&bytes, &mut cursor)?;
            if span == 0 {
                return Err("noncanonical singleton position");
            }
            span
        } else {
            0
        };
        result.push([start, start.checked_add(span).ok_or("position overflow")?]);
    }
    validate_positions(&result).map_err(|_| "invalid positional ranges")?;
    Ok(result)
}

pub(super) fn serialize<S: Serializer>(
    positions: &Positions,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    pack(positions)
        .map_err(serde::ser::Error::custom)?
        .serialize(serializer)
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Positions, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Representation {
        Compact(String),
        Legacy(Positions),
    }
    match Representation::deserialize(deserializer)? {
        Representation::Compact(value) => unpack(&value).map_err(serde::de::Error::custom),
        Representation::Legacy(positions) => {
            validate_positions(&positions).map_err(serde::de::Error::custom)?;
            Ok(positions)
        }
    }
}

struct Reference<'a>(&'a Positions);
impl Serialize for Reference<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize(self.0, serializer)
    }
}
struct Owned(Positions);
impl<'de> Deserialize<'de> for Owned {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize(deserializer).map(Self)
    }
}

pub(super) mod map {
    use super::*;
    pub(crate) fn serialize<S: Serializer>(
        postings: &BTreeMap<String, Positions>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(postings.len()))?;
        for (term, positions) in postings {
            map.serialize_entry(term, &Reference(positions))?;
        }
        map.end()
    }
    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<String, Positions>, D::Error> {
        BTreeMap::<String, Owned>::deserialize(deserializer).map(|map| {
            map.into_iter()
                .map(|(term, positions)| (term, positions.0))
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_positions_round_trip_and_reject_ambiguous_encodings() {
        for positions in [
            vec![[0, 399999]],
            vec![[0, 0], [2, 2], [10, 15], [127, 127], [100000, 100500]],
            (0..100000)
                .map(|index| [index * 997, index * 997])
                .collect(),
        ] {
            let encoded = pack(&positions).unwrap();
            assert_eq!(unpack(&encoded).unwrap(), positions);
        }
        assert!(pack(&vec![[0, 399999]]).unwrap().len() < 20);
        for bad in [
            "p2:AA==", "p1:", "p1:A", "p1:AB==", "p1:AA=A", "p1:gAA=", "p1:AQA=", "p1:AAA=",
        ] {
            assert!(unpack(bad).is_err(), "{bad}");
        }
    }
}
