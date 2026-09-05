//! Bounded, explicit capability extension negotiation.

use crate::{ERROR_EXTENSION_UNSUPPORTED, ProtocolError};

pub const MAX_EXTENSIONS: usize = 32;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Extensions {
    pub supported: Vec<u16>,
    pub required: Vec<u16>,
}

impl Extensions {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        for ids in [&self.supported, &self.required] {
            if ids.len() > MAX_EXTENSIONS
                || ids.iter().any(|id| *id == 0 || *id == u16::MAX)
                || ids.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(ProtocolError::frame("invalid extension identifier list"));
            }
        }
        if !contains_all(&self.supported, &self.required) {
            return Err(ProtocolError::frame(
                "required extension not advertised as supported",
            ));
        }
        Ok(())
    }

    pub fn negotiate(&self, peer: &Self) -> Result<Self, ProtocolError> {
        self.validate()?;
        peer.validate()?;
        if !contains_all(&self.supported, &peer.required)
            || !contains_all(&peer.supported, &self.required)
        {
            return Err(unsupported());
        }
        let supported = self
            .supported
            .iter()
            .copied()
            .filter(|id| peer.supported.binary_search(id).is_ok())
            .collect();
        let mut required = self.required.clone();
        required.extend_from_slice(&peer.required);
        required.sort_unstable();
        required.dedup();
        Ok(Self {
            supported,
            required,
        })
    }

    /// Validate the server's selected set against the original client offer.
    pub fn validate_response(&self, response: &Self) -> Result<(), ProtocolError> {
        self.validate()?;
        response.validate()?;
        if !contains_all(&self.supported, &response.supported) {
            return Err(ProtocolError::frame(
                "server selected an unoffered extension",
            ));
        }
        if !contains_all(&response.supported, &self.required) {
            return Err(unsupported());
        }
        if !contains_all(&response.required, &self.required) {
            return Err(ProtocolError::frame("server omitted a client requirement"));
        }
        Ok(())
    }
}

fn contains_all(haystack: &[u16], needles: &[u16]) -> bool {
    needles.iter().all(|id| haystack.binary_search(id).is_ok())
}

fn unsupported() -> ProtocolError {
    ProtocolError::new(
        ERROR_EXTENSION_UNSUPPORTED,
        "PIPESTREAM_EXTENSION_UNSUPPORTED",
        "a required extension is not supported by both peers",
    )
}

#[cfg(test)]
mod tests {
    use crate::{decode_capabilities, decode_ucf, encode_capabilities};

    fn unhex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    #[test]
    fn shared_extension_negotiation_vectors() {
        for row in include_str!("../../../test-vectors/extension-negotiation.tsv")
            .lines()
            .skip(1)
        {
            let fields: Vec<_> = row.split('\t').collect();
            assert_eq!(fields.len(), 5);
            let result = (|| {
                let peer = decode_capabilities(&unhex(fields[3]))?;
                if fields[1] == "decode" {
                    return Ok(String::from("ok"));
                }
                let local = decode_capabilities(&unhex(fields[2]))?;
                if fields[1] == "response" {
                    local.validate_response(&peer)?;
                    return Ok(String::from("ok"));
                }
                assert_eq!(fields[1], "negotiate");
                let frame = encode_capabilities(&local.negotiate(&peer)?)?;
                Ok(decode_ucf(&frame)?
                    .1
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect())
            })();
            let actual = result.unwrap_or_else(|error: crate::ProtocolError| error.name.to_owned());
            assert_eq!(actual, fields[4], "{}", fields[0]);
        }
    }
}
