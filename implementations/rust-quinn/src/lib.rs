use minicbor::{Decoder, Encoder};
use sha2::{Digest, Sha256};
use std::fmt;

pub mod transport;

pub const ALPN: &[u8] = b"pipestream/1";
pub const FRAME_STATUS: u8 = 0x50;
pub const FRAME_GOAWAY: u8 = 0x56;
pub const FRAME_CAPABILITIES: u8 = 0x80;
pub const FRAME_CHECKPOINT: u8 = 0x81;

pub const STATUS_UNSPECIFIED: u8 = 0;
pub const STATUS_PENDING: u8 = 1;
pub const STATUS_PROCESSING: u8 = 2;
pub const STATUS_COMPLETE: u8 = 3;
pub const STATUS_FAILED: u8 = 4;
pub const CHECKPOINT_ACK: u8 = 1;

pub const MAX_ENTITY_ID: u32 = 0xffff_fffc;
pub const CONNECTION_LEVEL: u32 = 0xffff_ffff;
pub const MAX_WINDOW: u32 = 0x7fff_fffe;
pub const MAX_CONTROL_FRAME: usize = 1 << 20;
pub const MAX_ENTITY_HEADER: usize = 1 << 16;
pub const MAX_PAYLOAD: usize = 64 << 20;

pub const ERROR_NO_ERROR: u32 = 0x00;
pub const ERROR_INTEGRITY: u32 = 0x04;
pub const ERROR_ENTITY_INVALID: u32 = 0x05;
pub const ERROR_LIMIT_EXCEEDED: u32 = 0x06;
pub const ERROR_LAYER_UNSUPPORTED: u32 = 0x0c;
pub const ERROR_FRAME: u32 = 0x0d;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError {
    pub code: u32,
    pub name: &'static str,
    pub detail: String,
}

impl ProtocolError {
    pub fn new(code: u32, name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            name,
            detail: detail.into(),
        }
    }

    fn frame(detail: impl Into<String>) -> Self {
        Self::new(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", detail)
    }

    fn entity(detail: impl Into<String>) -> Self {
        Self::new(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", detail)
    }

    fn integrity(detail: impl Into<String>) -> Self {
        Self::new(ERROR_INTEGRITY, "PIPESTREAM_INTEGRITY_ERROR", detail)
    }

    fn limit(detail: impl Into<String>) -> Self {
        Self::new(ERROR_LIMIT_EXCEEDED, "PIPESTREAM_LIMIT_EXCEEDED", detail)
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.name, self.detail)
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capabilities {
    pub layer0_core: bool,
    pub layer1_recursive: bool,
    pub layer2_resilience: bool,
    pub max_window_size: u32,
    pub serialization_format: u8,
    pub keepalive_timeout_ms: u64,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            layer0_core: true,
            layer1_recursive: false,
            layer2_resilience: false,
            max_window_size: 1024,
            serialization_format: 0,
            keepalive_timeout_ms: 30_000,
        }
    }
}

impl Capabilities {
    pub fn negotiate(&self, peer: &Self) -> Result<Self, ProtocolError> {
        if !peer.layer0_core {
            return Err(ProtocolError::new(
                ERROR_LAYER_UNSUPPORTED,
                "PIPESTREAM_LAYER_UNSUPPORTED",
                "Layer 0 is mandatory",
            ));
        }
        if peer.max_window_size == 0 || peer.max_window_size > MAX_WINDOW {
            return Err(ProtocolError::limit("invalid max-window-size"));
        }
        Ok(Self {
            layer0_core: true,
            layer1_recursive: self.layer1_recursive && peer.layer1_recursive,
            layer2_resilience: self.layer2_resilience
                && peer.layer2_resilience
                && self.layer1_recursive
                && peer.layer1_recursive,
            max_window_size: self.max_window_size.min(peer.max_window_size),
            serialization_format: 0,
            keepalive_timeout_ms: self.keepalive_timeout_ms.min(peer.keepalive_timeout_ms),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityHeader {
    pub entity_id: u32,
    pub parent_id: Option<u32>,
    pub layer: u8,
    pub content_type: Option<String>,
    pub payload_length: Option<u64>,
    pub checksum: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    pub state: u8,
    pub entity_id: u32,
    pub scope_id: u32,
    pub cursor: Option<u32>,
    pub depth: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub checkpoint_id: String,
    pub sequence_number: u64,
    pub checkpoint_entity_id: u32,
    pub scope_id: Option<u32>,
    pub flags: u8,
    pub timeout_ms: Option<u64>,
}

pub fn encode_ucf(frame_type: u8, payload: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    let length = u32::try_from(payload.len())
        .map_err(|_| ProtocolError::limit("control frame exceeds uint32"))?;
    let mut output = Vec::with_capacity(5 + payload.len());
    output.push(frame_type);
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(payload);
    Ok(output)
}

pub fn decode_ucf(data: &[u8]) -> Result<(u8, &[u8]), ProtocolError> {
    if data.len() < 5 {
        return Err(ProtocolError::frame("truncated UCF header"));
    }
    let length = u32::from_be_bytes(data[1..5].try_into().expect("slice length")) as usize;
    if length > MAX_CONTROL_FRAME {
        return Err(ProtocolError::limit("control frame exceeds local limit"));
    }
    if data.len() != length + 5 {
        return Err(ProtocolError::frame("UCF length does not match payload"));
    }
    Ok((data[0], &data[5..]))
}

pub fn encode_capabilities(capabilities: &Capabilities) -> Result<Vec<u8>, ProtocolError> {
    let mut body = Vec::new();
    let mut encoder = Encoder::new(&mut body);
    encoder.map(6).map_err(cbor_encode)?;
    encoder
        .str("layer0-core")
        .map_err(cbor_encode)?
        .bool(capabilities.layer0_core)
        .map_err(cbor_encode)?;
    encoder
        .str("max-window-size")
        .map_err(cbor_encode)?
        .u32(capabilities.max_window_size)
        .map_err(cbor_encode)?;
    encoder
        .str("layer1-recursive")
        .map_err(cbor_encode)?
        .bool(capabilities.layer1_recursive)
        .map_err(cbor_encode)?;
    encoder
        .str("layer2-resilience")
        .map_err(cbor_encode)?
        .bool(capabilities.layer2_resilience)
        .map_err(cbor_encode)?;
    encoder
        .str("keepalive-timeout-ms")
        .map_err(cbor_encode)?
        .u64(capabilities.keepalive_timeout_ms)
        .map_err(cbor_encode)?;
    encoder
        .str("serialization-format")
        .map_err(cbor_encode)?
        .u8(capabilities.serialization_format)
        .map_err(cbor_encode)?;
    encode_ucf(FRAME_CAPABILITIES, &body)
}

pub fn decode_capabilities(payload: &[u8]) -> Result<Capabilities, ProtocolError> {
    let mut decoder = Decoder::new(payload);
    let count = decoder
        .map()
        .map_err(cbor_decode)?
        .ok_or_else(|| ProtocolError::frame("indefinite maps are not permitted"))?;
    let mut layer0 = None;
    let mut layer1 = None;
    let mut layer2 = None;
    let mut max_window = MAX_WINDOW;
    let mut serialization = 0;
    let mut keepalive = 30_000;
    for _ in 0..count {
        let key = decoder.str().map_err(cbor_decode)?;
        match key {
            "layer0-core" => layer0 = Some(decoder.bool().map_err(cbor_decode)?),
            "layer1-recursive" => layer1 = Some(decoder.bool().map_err(cbor_decode)?),
            "layer2-resilience" => layer2 = Some(decoder.bool().map_err(cbor_decode)?),
            "max-window-size" => max_window = decoder.u32().map_err(cbor_decode)?,
            "serialization-format" => serialization = decoder.u8().map_err(cbor_decode)?,
            "keepalive-timeout-ms" => keepalive = decoder.u64().map_err(cbor_decode)?,
            _ => {
                return Err(ProtocolError::frame(format!(
                    "unknown capabilities field {key}"
                )));
            }
        }
    }
    if decoder.position() != payload.len() {
        return Err(ProtocolError::frame("trailing CBOR octets"));
    }
    let result = Capabilities {
        layer0_core: layer0.ok_or_else(|| ProtocolError::frame("missing layer0-core"))?,
        layer1_recursive: layer1.ok_or_else(|| ProtocolError::frame("missing layer1-recursive"))?,
        layer2_resilience: layer2
            .ok_or_else(|| ProtocolError::frame("missing layer2-resilience"))?,
        max_window_size: max_window,
        serialization_format: serialization,
        keepalive_timeout_ms: keepalive,
    };
    if !result.layer0_core {
        return Err(ProtocolError::new(
            ERROR_LAYER_UNSUPPORTED,
            "PIPESTREAM_LAYER_UNSUPPORTED",
            "Layer 0 is mandatory",
        ));
    }
    if result.max_window_size == 0 || result.max_window_size > MAX_WINDOW {
        return Err(ProtocolError::limit("invalid max-window-size"));
    }
    let canonical = encode_capabilities(&result)?;
    if canonical[5..] != *payload {
        return Err(ProtocolError::frame(
            "capabilities CBOR is not deterministic",
        ));
    }
    Ok(result)
}

pub fn encode_checkpoint(checkpoint: &Checkpoint) -> Result<Vec<u8>, ProtocolError> {
    let fields = 4
        + usize::from(checkpoint.scope_id.is_some())
        + usize::from(checkpoint.timeout_ms.is_some());
    let mut body = Vec::new();
    let mut encoder = Encoder::new(&mut body);
    encoder.map(fields as u64).map_err(cbor_encode)?;
    encoder
        .str("flags")
        .map_err(cbor_encode)?
        .u8(checkpoint.flags)
        .map_err(cbor_encode)?;
    if let Some(scope_id) = checkpoint.scope_id {
        encoder
            .str("scope-id")
            .map_err(cbor_encode)?
            .u32(scope_id)
            .map_err(cbor_encode)?;
    }
    if let Some(timeout_ms) = checkpoint.timeout_ms {
        encoder
            .str("timeout-ms")
            .map_err(cbor_encode)?
            .u64(timeout_ms)
            .map_err(cbor_encode)?;
    }
    encoder
        .str("checkpoint-id")
        .map_err(cbor_encode)?
        .str(&checkpoint.checkpoint_id)
        .map_err(cbor_encode)?;
    encoder
        .str("sequence-number")
        .map_err(cbor_encode)?
        .u64(checkpoint.sequence_number)
        .map_err(cbor_encode)?;
    encoder
        .str("checkpoint-entity-id")
        .map_err(cbor_encode)?
        .u32(checkpoint.checkpoint_entity_id)
        .map_err(cbor_encode)?;
    encode_ucf(FRAME_CHECKPOINT, &body)
}

pub fn decode_checkpoint(payload: &[u8]) -> Result<Checkpoint, ProtocolError> {
    let mut decoder = Decoder::new(payload);
    let count = decoder
        .map()
        .map_err(cbor_decode)?
        .ok_or_else(|| ProtocolError::frame("indefinite maps are not permitted"))?;
    let mut checkpoint_id = None;
    let mut sequence_number = None;
    let mut checkpoint_entity_id = None;
    let mut scope_id = None;
    let mut flags = 0;
    let mut timeout_ms = None;
    for _ in 0..count {
        let key = decoder.str().map_err(cbor_decode)?;
        match key {
            "checkpoint-id" => checkpoint_id = Some(decoder.str().map_err(cbor_decode)?.to_owned()),
            "sequence-number" => sequence_number = Some(decoder.u64().map_err(cbor_decode)?),
            "checkpoint-entity-id" => {
                checkpoint_entity_id = Some(decoder.u32().map_err(cbor_decode)?)
            }
            "scope-id" => scope_id = Some(decoder.u32().map_err(cbor_decode)?),
            "flags" => flags = decoder.u8().map_err(cbor_decode)?,
            "timeout-ms" => timeout_ms = Some(decoder.u64().map_err(cbor_decode)?),
            _ => {
                return Err(ProtocolError::frame(format!(
                    "unknown checkpoint field {key}"
                )));
            }
        }
    }
    if decoder.position() != payload.len() {
        return Err(ProtocolError::frame("trailing checkpoint CBOR octets"));
    }
    let result = Checkpoint {
        checkpoint_id: checkpoint_id
            .filter(|value| !value.is_empty() && value.len() <= 256)
            .ok_or_else(|| ProtocolError::frame("invalid checkpoint-id"))?,
        sequence_number: sequence_number
            .ok_or_else(|| ProtocolError::frame("missing sequence-number"))?,
        checkpoint_entity_id: checkpoint_entity_id
            .ok_or_else(|| ProtocolError::frame("missing checkpoint-entity-id"))?,
        scope_id,
        flags,
        timeout_ms,
    };
    if result.checkpoint_entity_id == 0 || result.checkpoint_entity_id > MAX_ENTITY_ID {
        return Err(ProtocolError::entity("invalid checkpoint-entity-id"));
    }
    if result.scope_id.is_some_and(|scope| scope != 0) {
        return Err(ProtocolError::new(
            ERROR_LAYER_UNSUPPORTED,
            "PIPESTREAM_LAYER_UNSUPPORTED",
            "checkpoint scope requires Layer 1",
        ));
    }
    if result.flags > CHECKPOINT_ACK {
        return Err(ProtocolError::frame("unknown checkpoint flags"));
    }
    let canonical = encode_checkpoint(&result)?;
    if canonical[5..] != *payload {
        return Err(ProtocolError::frame("checkpoint CBOR is not deterministic"));
    }
    Ok(result)
}

pub fn encode_status(status: Status) -> Result<Vec<u8>, ProtocolError> {
    if status.depth > 7 {
        return Err(ProtocolError::entity("depth exceeds 7"));
    }
    let mut word =
        (1u32 << 28) | ((u32::from(status.state) & 0xf) << 24) | (u32::from(status.depth) << 19);
    if status.cursor.is_some() {
        word |= 1 << 22;
    }
    let mut payload = Vec::with_capacity(if status.cursor.is_some() { 20 } else { 16 });
    payload.extend_from_slice(&word.to_be_bytes());
    payload.extend_from_slice(&status.entity_id.to_be_bytes());
    payload.extend_from_slice(&status.scope_id.to_be_bytes());
    payload.extend_from_slice(&0u32.to_be_bytes());
    if let Some(cursor) = status.cursor {
        payload.extend_from_slice(&cursor.to_be_bytes());
    }
    encode_ucf(FRAME_STATUS, &payload)
}

pub fn decode_status(payload: &[u8]) -> Result<Status, ProtocolError> {
    if payload.len() != 16 && payload.len() != 20 {
        return Err(ProtocolError::frame("invalid STATUS payload length"));
    }
    let word = u32::from_be_bytes(payload[..4].try_into().expect("slice length"));
    let version = word >> 28;
    if version != 1 {
        return Err(ProtocolError::new(
            ERROR_LAYER_UNSUPPORTED,
            "PIPESTREAM_LAYER_UNSUPPORTED",
            "unsupported STATUS version",
        ));
    }
    if word & (1 << 23) != 0 {
        return Err(ProtocolError::frame(
            "Layer 0 STATUS cannot carry extensions",
        ));
    }
    let has_cursor = word & (1 << 22) != 0;
    if has_cursor != (payload.len() == 20) {
        return Err(ProtocolError::frame(
            "STATUS cursor flag and length disagree",
        ));
    }
    let state = ((word >> 24) & 0xf) as u8;
    let entity_id = u32::from_be_bytes(payload[4..8].try_into().expect("slice length"));
    let scope_id = u32::from_be_bytes(payload[8..12].try_into().expect("slice length"));
    let depth = ((word >> 19) & 0x7) as u8;
    if depth != 0 || scope_id != 0 {
        return Err(ProtocolError::new(
            ERROR_LAYER_UNSUPPORTED,
            "PIPESTREAM_LAYER_UNSUPPORTED",
            "scope fields require Layer 1",
        ));
    }
    if state >= 8 {
        return Err(ProtocolError::new(
            ERROR_LAYER_UNSUPPORTED,
            "PIPESTREAM_LAYER_UNSUPPORTED",
            "status requires Layer 2",
        ));
    }
    if state == STATUS_UNSPECIFIED && entity_id != CONNECTION_LEVEL {
        return Err(ProtocolError::entity(
            "UNSPECIFIED is connection-level only",
        ));
    }
    let cursor =
        has_cursor.then(|| u32::from_be_bytes(payload[16..20].try_into().expect("slice length")));
    if cursor.is_some()
        && (state != STATUS_UNSPECIFIED
            || entity_id != CONNECTION_LEVEL
            || scope_id != 0
            || depth != 0)
    {
        return Err(ProtocolError::entity(
            "cursor update must be connection-level",
        ));
    }
    Ok(Status {
        state,
        entity_id,
        scope_id,
        cursor,
        depth,
    })
}

pub fn encode_goaway(last_entity_id: u32) -> Result<Vec<u8>, ProtocolError> {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&0u32.to_be_bytes());
    payload.extend_from_slice(&last_entity_id.to_be_bytes());
    encode_ucf(FRAME_GOAWAY, &payload)
}

pub fn decode_goaway(payload: &[u8]) -> Result<u32, ProtocolError> {
    if payload.len() != 8 {
        return Err(ProtocolError::frame("invalid GOAWAY payload length"));
    }
    Ok(u32::from_be_bytes(
        payload[4..8].try_into().expect("slice length"),
    ))
}

pub fn next_entity_id(current: u32) -> Result<u32, ProtocolError> {
    if current == 0 || current > MAX_ENTITY_ID {
        return Err(ProtocolError::entity("entity-id is reserved"));
    }
    Ok(if current == MAX_ENTITY_ID {
        1
    } else {
        current + 1
    })
}

pub fn encode_entity(header: &EntityHeader, payload: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    validate_entity_header(header, payload)?;
    let fields = 2
        + usize::from(header.parent_id.is_some())
        + usize::from(header.content_type.is_some())
        + usize::from(header.payload_length.is_some())
        + usize::from(header.checksum.is_some());
    let mut encoded = Vec::new();
    let mut encoder = Encoder::new(&mut encoded);
    encoder.map(fields as u64).map_err(cbor_encode)?;
    encoder
        .str("layer")
        .map_err(cbor_encode)?
        .u8(header.layer)
        .map_err(cbor_encode)?;
    if let Some(checksum) = header.checksum {
        encoder
            .str("checksum")
            .map_err(cbor_encode)?
            .bytes(&checksum)
            .map_err(cbor_encode)?;
    }
    encoder
        .str("entity-id")
        .map_err(cbor_encode)?
        .u32(header.entity_id)
        .map_err(cbor_encode)?;
    if let Some(parent_id) = header.parent_id {
        encoder
            .str("parent-id")
            .map_err(cbor_encode)?
            .u32(parent_id)
            .map_err(cbor_encode)?;
    }
    if let Some(content_type) = &header.content_type {
        encoder
            .str("content-type")
            .map_err(cbor_encode)?
            .str(content_type)
            .map_err(cbor_encode)?;
    }
    if let Some(length) = header.payload_length {
        encoder
            .str("payload-length")
            .map_err(cbor_encode)?
            .u64(length)
            .map_err(cbor_encode)?;
    }
    let header_length = u32::try_from(encoded.len())
        .map_err(|_| ProtocolError::limit("entity header exceeds uint32"))?;
    let mut output = Vec::with_capacity(4 + encoded.len() + payload.len());
    output.extend_from_slice(&header_length.to_be_bytes());
    output.extend_from_slice(&encoded);
    output.extend_from_slice(payload);
    Ok(output)
}

pub fn entity(
    entity_id: u32,
    payload: &[u8],
    content_type: &str,
) -> Result<Vec<u8>, ProtocolError> {
    entity_with_parent(entity_id, None, payload, content_type)
}

pub fn entity_with_parent(
    entity_id: u32,
    parent_id: Option<u32>,
    payload: &[u8],
    content_type: &str,
) -> Result<Vec<u8>, ProtocolError> {
    let checksum: [u8; 32] = Sha256::digest(payload).into();
    encode_entity(
        &EntityHeader {
            entity_id,
            parent_id,
            layer: 0,
            content_type: Some(content_type.to_owned()),
            payload_length: Some(payload.len() as u64),
            checksum: Some(checksum),
        },
        payload,
    )
}

pub fn decode_entity(data: &[u8]) -> Result<(EntityHeader, &[u8]), ProtocolError> {
    if data.len() < 4 {
        return Err(ProtocolError::frame("truncated entity header length"));
    }
    let header_length = u32::from_be_bytes(data[..4].try_into().expect("slice length")) as usize;
    if header_length > MAX_ENTITY_HEADER {
        return Err(ProtocolError::limit("entity header exceeds local limit"));
    }
    if data.len() < 4 + header_length {
        return Err(ProtocolError::frame("truncated entity header"));
    }
    let encoded = &data[4..4 + header_length];
    let payload = &data[4 + header_length..];
    if payload.len() > MAX_PAYLOAD {
        return Err(ProtocolError::limit("entity payload exceeds local limit"));
    }
    let mut decoder = Decoder::new(encoded);
    let count = decoder
        .map()
        .map_err(cbor_decode)?
        .ok_or_else(|| ProtocolError::frame("indefinite maps are not permitted"))?;
    let mut entity_id = None;
    let mut parent_id = None;
    let mut layer = None;
    let mut content_type = None;
    let mut payload_length = None;
    let mut checksum = None;
    for _ in 0..count {
        let key = decoder.str().map_err(cbor_decode)?;
        match key {
            "entity-id" => entity_id = Some(decoder.u32().map_err(cbor_decode)?),
            "parent-id" => parent_id = Some(decoder.u32().map_err(cbor_decode)?),
            "layer" => layer = Some(decoder.u8().map_err(cbor_decode)?),
            "content-type" => content_type = Some(decoder.str().map_err(cbor_decode)?.to_owned()),
            "payload-length" => payload_length = Some(decoder.u64().map_err(cbor_decode)?),
            "checksum" => {
                let bytes = decoder.bytes().map_err(cbor_decode)?;
                checksum =
                    Some(bytes.try_into().map_err(|_| {
                        ProtocolError::integrity("checksum must contain 32 octets")
                    })?);
            }
            _ => {
                return Err(ProtocolError::frame(format!(
                    "unsupported Layer 0 entity field {key}"
                )));
            }
        }
    }
    if decoder.position() != encoded.len() {
        return Err(ProtocolError::frame("trailing entity header CBOR octets"));
    }
    let header = EntityHeader {
        entity_id: entity_id.ok_or_else(|| ProtocolError::entity("entity-id is absent"))?,
        parent_id,
        layer: layer.ok_or_else(|| ProtocolError::entity("layer is absent"))?,
        content_type,
        payload_length,
        checksum,
    };
    validate_entity_header(&header, payload)?;
    let canonical = encode_entity(&header, payload)?;
    if canonical[4..4 + header_length] != *encoded {
        return Err(ProtocolError::frame(
            "entity header CBOR is not deterministic",
        ));
    }
    Ok((header, payload))
}

fn validate_entity_header(header: &EntityHeader, payload: &[u8]) -> Result<(), ProtocolError> {
    if header.entity_id == 0 || header.entity_id > MAX_ENTITY_ID {
        return Err(ProtocolError::entity("entity-id is reserved"));
    }
    if header
        .parent_id
        .is_some_and(|parent_id| parent_id == 0 || parent_id > MAX_ENTITY_ID)
    {
        return Err(ProtocolError::entity("parent-id is reserved or invalid"));
    }
    if header.layer > 3 {
        return Err(ProtocolError::entity("layer must be 0 through 3"));
    }
    if header
        .payload_length
        .is_some_and(|length| length != payload.len() as u64)
    {
        return Err(ProtocolError::entity("payload-length mismatch"));
    }
    if let Some(expected) = header.checksum {
        let actual: [u8; 32] = Sha256::digest(payload).into();
        if expected != actual {
            return Err(ProtocolError::integrity("checksum mismatch"));
        }
    }
    Ok(())
}

fn cbor_encode(error: minicbor::encode::Error<std::convert::Infallible>) -> ProtocolError {
    ProtocolError::frame(format!("CBOR encode failed: {error}"))
}

fn cbor_decode(error: minicbor::decode::Error) -> ProtocolError {
    ProtocolError::frame(format!("CBOR decode failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};

    #[test]
    fn capabilities_match_golden_vector() {
        let expected = include_bytes!("../../../test-vectors/valid/capabilities-default.bin");
        assert_eq!(
            expected.as_slice(),
            encode_capabilities(&Capabilities::default()).unwrap()
        );
    }

    #[test]
    fn entity_matches_golden_vector() {
        let expected = include_bytes!("../../../test-vectors/valid/entity-text.bin");
        assert_eq!(
            expected.as_slice(),
            entity(7, b"PipeStream Layer 0\n", "text/plain; charset=utf-8").unwrap()
        );
    }

    #[test]
    fn entire_corpus_has_expected_acceptance_and_named_refusals() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test-vectors");
        let index = fs::read_to_string(root.join("index.tsv")).unwrap();
        for row in index.lines().skip(1) {
            let fields: Vec<_> = row.split('\t').collect();
            let name = fields[0];
            let expectation = fields[2];
            let bytes = fs::read(root.join(expectation).join(format!("{name}.bin"))).unwrap();
            let result = decode_named(name, &bytes);
            if expectation == "valid" {
                result.unwrap_or_else(|error| panic!("{name}: {error}"));
            } else {
                assert_eq!(fields[3], result.unwrap_err().name, "{name}");
            }
        }
    }

    fn decode_named(name: &str, bytes: &[u8]) -> Result<(), ProtocolError> {
        if name.starts_with("entity-") {
            decode_entity(bytes)?;
            return Ok(());
        }
        let (frame_type, payload) = decode_ucf(bytes)?;
        if name.starts_with("capabilities-") || name.starts_with("cbor-") {
            assert_eq!(FRAME_CAPABILITIES, frame_type);
            decode_capabilities(payload)?;
        } else if name.starts_with("status-") {
            assert_eq!(FRAME_STATUS, frame_type);
            decode_status(payload)?;
        } else if name.starts_with("goaway") {
            assert_eq!(FRAME_GOAWAY, frame_type);
            decode_goaway(payload)?;
        } else if name.starts_with("checkpoint-") {
            assert_eq!(FRAME_CHECKPOINT, frame_type);
            decode_checkpoint(payload)?;
        }
        Ok(())
    }
}
