use minicbor::{Decoder, Encoder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt};

pub mod authorization;
pub use authorization::ERROR_UNAUTHORIZED;
mod deterministic;
pub mod execution;
pub mod extensions;
pub mod jobs;
pub mod persistence;
pub mod recovery;
pub mod session;
pub mod uri;
pub mod work_set;

pub const ALPN: &[u8] = b"pipestream/1";
pub const FRAME_STATUS: u8 = 0x50;
pub const FRAME_SCOPE_DIGEST: u8 = 0x54;
pub const FRAME_BARRIER: u8 = 0x55;
pub const FRAME_GOAWAY: u8 = 0x56;
pub const FRAME_CAPABILITIES: u8 = 0x80;
pub const FRAME_CHECKPOINT: u8 = 0x81;
pub const FRAME_CLAIM_REDEMPTION: u8 = 0x82;

pub const STATUS_UNSPECIFIED: u8 = 0;
pub const STATUS_PENDING: u8 = 1;
pub const STATUS_PROCESSING: u8 = 2;
pub const STATUS_COMPLETE: u8 = 3;
pub const STATUS_FAILED: u8 = 4;
pub const STATUS_CHECKPOINT: u8 = 5;
pub const STATUS_DEHYDRATING: u8 = 6;
pub const STATUS_REHYDRATING: u8 = 7;
pub const STATUS_YIELDED: u8 = 8;
pub const STATUS_DEFERRED: u8 = 9;
pub const STATUS_RETRYING: u8 = 10;
pub const STATUS_SKIPPED: u8 = 11;
pub const STATUS_ABANDONED: u8 = 12;
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
pub const ERROR_EXTENSION_UNSUPPORTED: u32 = 0x0f;
pub const ERROR_DEPTH_EXCEEDED: u32 = 0x07;
pub const ERROR_WINDOW_EXCEEDED: u32 = 0x08;
pub const ERROR_SCOPE_INVALID: u32 = 0x09;
pub const ERROR_CLAIM_EXPIRED: u32 = 0x0a;
pub const ERROR_CLAIM_NOT_FOUND: u32 = 0x0b;

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
    pub max_scope_depth: Option<u8>,
    pub max_entities_per_scope: Option<u32>,
    pub max_window_size: u32,
    pub serialization_format: u8,
    pub keepalive_timeout_ms: u64,
    pub extensions: extensions::Extensions,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            layer0_core: true,
            layer1_recursive: false,
            layer2_resilience: false,
            max_scope_depth: None,
            max_entities_per_scope: None,
            max_window_size: 1024,
            serialization_format: 0,
            keepalive_timeout_ms: 30_000,
            extensions: extensions::Extensions::default(),
        }
    }
}

impl Capabilities {
    pub const DEFAULT_MAX_SCOPE_DEPTH: u8 = 7;
    pub const DEFAULT_MAX_ENTITIES_PER_SCOPE: u32 = MAX_ENTITY_ID;

    #[must_use]
    pub fn effective_max_scope_depth(&self) -> u8 {
        self.max_scope_depth
            .unwrap_or(Self::DEFAULT_MAX_SCOPE_DEPTH)
    }

    #[must_use]
    pub fn effective_max_entities_per_scope(&self) -> u32 {
        self.max_entities_per_scope
            .unwrap_or(Self::DEFAULT_MAX_ENTITIES_PER_SCOPE)
    }

    pub fn negotiate(&self, peer: &Self) -> Result<Self, ProtocolError> {
        let extensions = self.extensions.negotiate(&peer.extensions)?;
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
        let layer1_recursive = self.layer1_recursive && peer.layer1_recursive;
        let layer2_resilience =
            self.layer2_resilience && peer.layer2_resilience && layer1_recursive;
        if extensions
            .supported
            .contains(&work_set::EXTENSION_SEALED_WORK_SETS)
            && (!layer1_recursive || layer2_resilience)
        {
            return Err(ProtocolError::new(
                ERROR_EXTENSION_UNSUPPORTED,
                "PIPESTREAM_EXTENSION_UNSUPPORTED",
                "sealed work sets require Layer 1 without Layer 2",
            ));
        }
        Ok(Self {
            layer0_core: true,
            layer1_recursive,
            layer2_resilience,
            max_scope_depth: layer1_recursive.then(|| {
                self.effective_max_scope_depth()
                    .min(peer.effective_max_scope_depth())
            }),
            max_entities_per_scope: layer1_recursive.then(|| {
                self.effective_max_entities_per_scope()
                    .min(peer.effective_max_entities_per_scope())
            }),
            max_window_size: self.max_window_size.min(peer.max_window_size),
            serialization_format: 0,
            keepalive_timeout_ms: self.keepalive_timeout_ms.min(peer.keepalive_timeout_ms),
            extensions,
        })
    }

    pub fn validate_response(&self, response: &Self) -> Result<(), ProtocolError> {
        self.extensions.validate_response(&response.extensions)?;
        if response
            .extensions
            .supported
            .contains(&work_set::EXTENSION_SEALED_WORK_SETS)
            && (!response.layer1_recursive || response.layer2_resilience)
        {
            return Err(ProtocolError::frame(
                "invalid sealed-work capability combination",
            ));
        }
        if !response.layer0_core
            || (response.layer1_recursive && !self.layer1_recursive)
            || (response.layer2_resilience
                && (!self.layer2_resilience || !response.layer1_recursive))
            || response.max_window_size == 0
            || response.max_window_size > self.max_window_size
            || response.effective_max_scope_depth() > self.effective_max_scope_depth()
                && response.layer1_recursive
            || response.effective_max_entities_per_scope() > self.effective_max_entities_per_scope()
                && response.layer1_recursive
            || response.keepalive_timeout_ms > self.keepalive_timeout_ms
            || response.serialization_format != 0
        {
            return Err(ProtocolError::frame("server exceeded offered capabilities"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayerSupport {
    pub layer1_recursive: bool,
    pub layer2_resilience: bool,
}

impl LayerSupport {
    pub const LAYER0: Self = Self {
        layer1_recursive: false,
        layer2_resilience: false,
    };
    pub const LAYER1: Self = Self {
        layer1_recursive: true,
        layer2_resilience: false,
    };
    pub const LAYER2: Self = Self {
        layer1_recursive: true,
        layer2_resilience: true,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub total_chunks: u64,
    pub chunk_index: u64,
    pub chunk_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompletionMode {
    Unspecified = 0,
    Strict = 1,
    Lenient = 2,
    BestEffort = 3,
    Quorum = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum FailureAction {
    Unspecified = 0,
    Fail = 1,
    Skip = 2,
    Retry = 3,
    Defer = 4,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CompletionPolicy {
    pub mode: Option<CompletionMode>,
    pub max_retries: Option<u64>,
    pub retry_delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub min_success_ratio: Option<f32>,
    pub on_timeout: Option<FailureAction>,
    pub on_failure: Option<FailureAction>,
}

impl CompletionPolicy {
    #[must_use]
    pub fn effective_mode(&self) -> CompletionMode {
        self.mode.unwrap_or(CompletionMode::Strict)
    }

    #[must_use]
    pub fn effective_max_retries(&self) -> u64 {
        self.max_retries.unwrap_or(3)
    }

    #[must_use]
    pub fn effective_retry_delay_ms(&self) -> u64 {
        self.retry_delay_ms.unwrap_or(1_000)
    }

    #[must_use]
    pub fn effective_timeout_ms(&self) -> u64 {
        self.timeout_ms.unwrap_or(300_000)
    }

    #[must_use]
    pub fn effective_on_timeout(&self) -> FailureAction {
        self.on_timeout.unwrap_or(FailureAction::Fail)
    }

    #[must_use]
    pub fn effective_on_failure(&self) -> FailureAction {
        self.on_failure.unwrap_or(FailureAction::Fail)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityHeader {
    pub entity_id: u32,
    pub parent_id: Option<u32>,
    pub scope_id: Option<u32>,
    pub parent_scope_id: Option<u32>,
    pub layer: u8,
    pub content_type: Option<String>,
    pub payload_length: Option<u64>,
    pub checksum: Option<[u8; 32]>,
    pub metadata: BTreeMap<String, String>,
    pub chunk_info: Option<ChunkInfo>,
    pub completion_policy: Option<CompletionPolicy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    pub state: u8,
    pub entity_id: u32,
    pub scope_id: u32,
    pub cursor: Option<u32>,
    pub depth: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoppingPointValidation {
    pub state_checksum: Option<[u8; 32]>,
    pub bytes_processed: Option<u64>,
    pub children_complete: Option<u64>,
    pub children_total: Option<u64>,
    pub is_resumable: Option<bool>,
    pub checkpoint_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusExtension {
    Yield {
        reason: u8,
        token: Vec<u8>,
    },
    ClaimCheck {
        claim_id: u64,
        expiry_timestamp_micros: u64,
    },
    Opaque(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusFrame {
    pub status: Status,
    pub extension: Option<StatusExtension>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeDigest {
    pub scope_id: u32,
    pub entities_processed: u64,
    pub entities_succeeded: u64,
    pub entities_failed: u64,
    pub entities_deferred: u64,
    pub merkle_root: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Barrier {
    pub released: bool,
    pub scope_id: u32,
    pub parent_entity_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRedemption {
    pub session_id: String,
    pub claim_id: u64,
    pub state_checksum: [u8; 32],
    pub acknowledged: bool,
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
    capabilities.extensions.validate()?;
    let mut body = Vec::new();
    let mut encoder = Encoder::new(&mut body);
    let fields = 6
        + usize::from(capabilities.max_scope_depth.is_some())
        + usize::from(capabilities.max_entities_per_scope.is_some())
        + usize::from(!capabilities.extensions.supported.is_empty())
        + usize::from(!capabilities.extensions.required.is_empty());
    encoder.map(fields as u64).map_err(cbor_encode)?;
    encoder
        .str("layer0-core")
        .map_err(cbor_encode)?
        .bool(capabilities.layer0_core)
        .map_err(cbor_encode)?;
    if let Some(max_scope_depth) = capabilities.max_scope_depth {
        encoder
            .str("max-scope-depth")
            .map_err(cbor_encode)?
            .u8(max_scope_depth)
            .map_err(cbor_encode)?;
    }
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
    encode_extensions(
        &mut encoder,
        "required-extensions",
        &capabilities.extensions.required,
    )?;
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
    encode_extensions(
        &mut encoder,
        "supported-extensions",
        &capabilities.extensions.supported,
    )?;
    if let Some(max_entities_per_scope) = capabilities.max_entities_per_scope {
        encoder
            .str("max-entities-per-scope")
            .map_err(cbor_encode)?
            .u32(max_entities_per_scope)
            .map_err(cbor_encode)?;
    }
    encode_ucf(FRAME_CAPABILITIES, &body)
}

fn encode_extensions(
    encoder: &mut Encoder<&mut Vec<u8>>,
    key: &str,
    ids: &[u16],
) -> Result<(), ProtocolError> {
    if !ids.is_empty() {
        encoder
            .str(key)
            .map_err(cbor_encode)?
            .array(ids.len() as u64)
            .map_err(cbor_encode)?;
        for id in ids {
            encoder.u16(*id).map_err(cbor_encode)?;
        }
    }
    Ok(())
}

fn decode_extensions(decoder: &mut Decoder<'_>) -> Result<Vec<u16>, ProtocolError> {
    let count = decoder
        .array()
        .map_err(cbor_decode)?
        .filter(|count| *count <= extensions::MAX_EXTENSIONS as u64)
        .ok_or_else(|| ProtocolError::frame("invalid extension array length"))?;
    (0..count)
        .map(|_| decoder.u16().map_err(cbor_decode))
        .collect()
}

pub fn decode_capabilities(payload: &[u8]) -> Result<Capabilities, ProtocolError> {
    deterministic::validate(payload)?;
    let mut decoder = Decoder::new(payload);
    let count = decoder
        .map()
        .map_err(cbor_decode)?
        .ok_or_else(|| ProtocolError::frame("indefinite maps are not permitted"))?;
    let mut layer0 = None;
    let mut layer1 = None;
    let mut layer2 = None;
    let mut max_scope_depth = None;
    let mut max_entities_per_scope = None;
    let mut max_window = MAX_WINDOW;
    let mut serialization = 0;
    let mut keepalive = 30_000;
    let mut extensions = extensions::Extensions::default();
    for _ in 0..count {
        let key = decoder.str().map_err(cbor_decode)?;
        match key {
            "layer0-core" => layer0 = Some(decoder.bool().map_err(cbor_decode)?),
            "layer1-recursive" => layer1 = Some(decoder.bool().map_err(cbor_decode)?),
            "layer2-resilience" => layer2 = Some(decoder.bool().map_err(cbor_decode)?),
            "max-scope-depth" => max_scope_depth = Some(decoder.u8().map_err(cbor_decode)?),
            "max-entities-per-scope" => {
                max_entities_per_scope = Some(decoder.u32().map_err(cbor_decode)?)
            }
            "max-window-size" => max_window = decoder.u32().map_err(cbor_decode)?,
            "serialization-format" => serialization = decoder.u8().map_err(cbor_decode)?,
            "keepalive-timeout-ms" => keepalive = decoder.u64().map_err(cbor_decode)?,
            "supported-extensions" => extensions.supported = decode_extensions(&mut decoder)?,
            "required-extensions" => extensions.required = decode_extensions(&mut decoder)?,
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
        max_scope_depth,
        max_entities_per_scope,
        max_window_size: max_window,
        serialization_format: serialization,
        keepalive_timeout_ms: keepalive,
        extensions,
    };
    result.extensions.validate()?;
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
    if result.max_scope_depth.is_some_and(|depth| depth > 7) {
        return Err(ProtocolError::new(
            ERROR_DEPTH_EXCEEDED,
            "PIPESTREAM_DEPTH_EXCEEDED",
            "max-scope-depth exceeds 7",
        ));
    }
    if result
        .max_entities_per_scope
        .is_some_and(|limit| limit == 0 || limit > MAX_ENTITY_ID)
    {
        return Err(ProtocolError::limit("invalid max-entities-per-scope"));
    }
    Ok(result)
}

pub fn encode_checkpoint(checkpoint: &Checkpoint) -> Result<Vec<u8>, ProtocolError> {
    encode_checkpoint_for(checkpoint, LayerSupport::LAYER0)
}

pub fn encode_checkpoint_for(
    checkpoint: &Checkpoint,
    layers: LayerSupport,
) -> Result<Vec<u8>, ProtocolError> {
    validate_checkpoint(checkpoint, layers)?;
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
    decode_checkpoint_for(payload, LayerSupport::LAYER0)
}

pub fn decode_checkpoint_for(
    payload: &[u8],
    layers: LayerSupport,
) -> Result<Checkpoint, ProtocolError> {
    deterministic::validate(payload)?;
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
    validate_checkpoint(&result, layers)?;
    Ok(result)
}

fn validate_checkpoint(checkpoint: &Checkpoint, layers: LayerSupport) -> Result<(), ProtocolError> {
    if checkpoint.checkpoint_id.is_empty() || checkpoint.checkpoint_id.len() > 256 {
        return Err(ProtocolError::frame("invalid checkpoint-id"));
    }
    if checkpoint.checkpoint_entity_id == 0 || checkpoint.checkpoint_entity_id > MAX_ENTITY_ID {
        return Err(ProtocolError::entity("invalid checkpoint-entity-id"));
    }
    if let Some(scope) = checkpoint.scope_id
        && scope != 0
        && !layers.layer1_recursive
    {
        return Err(ProtocolError::new(
            ERROR_LAYER_UNSUPPORTED,
            "PIPESTREAM_LAYER_UNSUPPORTED",
            "checkpoint scope requires Layer 1",
        ));
    }
    if checkpoint.flags > CHECKPOINT_ACK {
        return Err(ProtocolError::frame("unknown checkpoint flags"));
    }
    Ok(())
}

pub fn encode_status(status: Status) -> Result<Vec<u8>, ProtocolError> {
    encode_status_frame(
        &StatusFrame {
            status,
            extension: None,
        },
        LayerSupport::LAYER0,
    )
}

pub fn encode_status_frame(
    frame: &StatusFrame,
    layers: LayerSupport,
) -> Result<Vec<u8>, ProtocolError> {
    validate_status(&frame.status, frame.extension.as_ref(), layers)?;
    let status = frame.status;
    let mut word =
        (1u32 << 28) | ((u32::from(status.state) & 0xf) << 24) | (u32::from(status.depth) << 19);
    if frame.extension.is_some() {
        word |= 1 << 23;
    }
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
    if let Some(extension) = &frame.extension {
        let encoded = encode_status_extension(extension)?;
        payload.extend_from_slice(
            &u32::try_from(encoded.len())
                .map_err(|_| ProtocolError::limit("status extension exceeds uint32"))?
                .to_be_bytes(),
        );
        payload.extend_from_slice(&encoded);
    }
    encode_ucf(FRAME_STATUS, &payload)
}

pub fn decode_status(payload: &[u8]) -> Result<Status, ProtocolError> {
    Ok(decode_status_frame(payload, LayerSupport::LAYER0)?.status)
}

pub fn decode_status_frame(
    payload: &[u8],
    layers: LayerSupport,
) -> Result<StatusFrame, ProtocolError> {
    if payload.len() < 16 {
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
    let has_extension = word & (1 << 23) != 0;
    let has_cursor = word & (1 << 22) != 0;
    let state = ((word >> 24) & 0xf) as u8;
    let entity_id = u32::from_be_bytes(payload[4..8].try_into().expect("slice length"));
    let scope_id = u32::from_be_bytes(payload[8..12].try_into().expect("slice length"));
    let depth = ((word >> 19) & 0x7) as u8;
    let mut offset = 16;
    let cursor = if has_cursor {
        if payload.len() < offset + 4 {
            return Err(ProtocolError::frame("STATUS cursor is truncated"));
        }
        let value = u32::from_be_bytes(
            payload[offset..offset + 4]
                .try_into()
                .expect("slice length"),
        );
        offset += 4;
        Some(value)
    } else {
        None
    };
    let extension = if has_extension {
        if payload.len() < offset + 4 {
            return Err(ProtocolError::frame("STATUS extension header is truncated"));
        }
        let length = u32::from_be_bytes(
            payload[offset..offset + 4]
                .try_into()
                .expect("slice length"),
        ) as usize;
        offset += 4;
        if length == 0 || payload.len() != offset + length {
            return Err(ProtocolError::frame("STATUS extension length is invalid"));
        }
        Some(decode_status_extension(state, &payload[offset..])?)
    } else {
        if payload.len() != offset {
            return Err(ProtocolError::frame(
                "STATUS length contains unflagged trailing data",
            ));
        }
        None
    };
    let frame = StatusFrame {
        status: Status {
            state,
            entity_id,
            scope_id,
            cursor,
            depth,
        },
        extension,
    };
    validate_status(&frame.status, frame.extension.as_ref(), layers)?;
    Ok(frame)
}

fn encode_status_extension(extension: &StatusExtension) -> Result<Vec<u8>, ProtocolError> {
    match extension {
        StatusExtension::Yield { reason, token } => {
            if token.len() > 0x00ff_ffff {
                return Err(ProtocolError::entity("invalid yield extension"));
            }
            let length = token.len() as u32;
            let mut encoded = Vec::with_capacity(4 + token.len());
            encoded.push(*reason);
            let length_bytes = length.to_be_bytes();
            encoded.extend_from_slice(&length_bytes[1..]);
            encoded.extend_from_slice(token);
            Ok(encoded)
        }
        StatusExtension::ClaimCheck {
            claim_id,
            expiry_timestamp_micros,
        } => {
            if *claim_id == 0 || *expiry_timestamp_micros == 0 {
                return Err(ProtocolError::entity("invalid claim-check extension"));
            }
            let mut encoded = Vec::with_capacity(16);
            encoded.extend_from_slice(&claim_id.to_be_bytes());
            encoded.extend_from_slice(&expiry_timestamp_micros.to_be_bytes());
            Ok(encoded)
        }
        StatusExtension::Opaque(bytes) if !bytes.is_empty() => Ok(bytes.clone()),
        StatusExtension::Opaque(_) => Err(ProtocolError::frame("empty status extension")),
    }
}

fn decode_status_extension(state: u8, extension: &[u8]) -> Result<StatusExtension, ProtocolError> {
    match state {
        STATUS_YIELDED => {
            if extension.len() < 4 {
                return Err(ProtocolError::frame("yield extension is truncated"));
            }
            let reason = extension[0];
            let token_length =
                u32::from_be_bytes([0, extension[1], extension[2], extension[3]]) as usize;
            if extension.len() != 4 + token_length {
                return Err(ProtocolError::frame("yield extension is invalid"));
            }
            Ok(StatusExtension::Yield {
                reason,
                token: extension[4..].to_vec(),
            })
        }
        STATUS_DEFERRED => {
            if extension.len() != 16 {
                return Err(ProtocolError::frame("claim-check extension is invalid"));
            }
            let claim_id = u64::from_be_bytes(extension[..8].try_into().expect("slice length"));
            let expiry_timestamp_micros =
                u64::from_be_bytes(extension[8..].try_into().expect("slice length"));
            if claim_id == 0 || expiry_timestamp_micros == 0 {
                return Err(ProtocolError::entity("claim-check values are reserved"));
            }
            Ok(StatusExtension::ClaimCheck {
                claim_id,
                expiry_timestamp_micros,
            })
        }
        _ => Ok(StatusExtension::Opaque(extension.to_vec())),
    }
}

fn validate_status(
    status: &Status,
    extension: Option<&StatusExtension>,
    layers: LayerSupport,
) -> Result<(), ProtocolError> {
    if status.depth > 7 {
        return Err(ProtocolError::new(
            ERROR_DEPTH_EXCEEDED,
            "PIPESTREAM_DEPTH_EXCEEDED",
            "depth exceeds 7",
        ));
    }
    if status.state > STATUS_ABANDONED {
        return Err(ProtocolError::entity("unassigned status code"));
    }
    if status.state >= STATUS_YIELDED && !layers.layer2_resilience {
        return Err(ProtocolError::new(
            ERROR_LAYER_UNSUPPORTED,
            "PIPESTREAM_LAYER_UNSUPPORTED",
            "status requires Layer 2",
        ));
    }
    if (status.depth != 0 || status.scope_id != 0) && !layers.layer1_recursive {
        return Err(ProtocolError::new(
            ERROR_LAYER_UNSUPPORTED,
            "PIPESTREAM_LAYER_UNSUPPORTED",
            "scope fields require Layer 1",
        ));
    }
    if (status.scope_id == 0) != (status.depth == 0) {
        return Err(ProtocolError::new(
            ERROR_SCOPE_INVALID,
            "PIPESTREAM_SCOPE_INVALID",
            "scope ID and depth disagree",
        ));
    }
    if status.state == STATUS_UNSPECIFIED && status.entity_id != CONNECTION_LEVEL {
        return Err(ProtocolError::entity(
            "UNSPECIFIED is connection-level only",
        ));
    }
    if status.cursor.is_some()
        && (status.state != STATUS_UNSPECIFIED
            || status.entity_id != CONNECTION_LEVEL
            || status.scope_id != 0
            || status.depth != 0)
    {
        return Err(ProtocolError::entity(
            "cursor update must be connection-level",
        ));
    }
    if status
        .cursor
        .is_some_and(|cursor| cursor == 0 || cursor > MAX_ENTITY_ID)
    {
        return Err(ProtocolError::entity("cursor is reserved"));
    }
    match (status.state, extension) {
        (STATUS_YIELDED, Some(StatusExtension::Yield { .. }))
        | (STATUS_DEFERRED, Some(StatusExtension::ClaimCheck { .. })) => Ok(()),
        (STATUS_YIELDED | STATUS_DEFERRED, _) => Err(ProtocolError::entity(
            "Layer 2 status requires its defined extension",
        )),
        (_, None) | (_, Some(StatusExtension::Opaque(_))) => Ok(()),
        (_, Some(StatusExtension::Yield { .. } | StatusExtension::ClaimCheck { .. })) => Err(
            ProtocolError::entity("status extension type does not match state"),
        ),
    }
}

pub fn encode_scope_digest(digest: &ScopeDigest) -> Result<Vec<u8>, ProtocolError> {
    validate_scope_digest(digest)?;
    let mut payload = Vec::with_capacity(72);
    payload.extend_from_slice(&0u32.to_be_bytes());
    payload.extend_from_slice(&digest.scope_id.to_be_bytes());
    payload.extend_from_slice(&digest.entities_processed.to_be_bytes());
    payload.extend_from_slice(&digest.entities_succeeded.to_be_bytes());
    payload.extend_from_slice(&digest.entities_failed.to_be_bytes());
    payload.extend_from_slice(&digest.entities_deferred.to_be_bytes());
    payload.extend_from_slice(&digest.merkle_root);
    encode_ucf(FRAME_SCOPE_DIGEST, &payload)
}

pub fn decode_scope_digest(payload: &[u8]) -> Result<ScopeDigest, ProtocolError> {
    if payload.len() != 72 {
        return Err(ProtocolError::frame("invalid SCOPE_DIGEST payload length"));
    }
    let digest = ScopeDigest {
        scope_id: u32::from_be_bytes(payload[4..8].try_into().expect("slice length")),
        entities_processed: u64::from_be_bytes(payload[8..16].try_into().expect("slice length")),
        entities_succeeded: u64::from_be_bytes(payload[16..24].try_into().expect("slice length")),
        entities_failed: u64::from_be_bytes(payload[24..32].try_into().expect("slice length")),
        entities_deferred: u64::from_be_bytes(payload[32..40].try_into().expect("slice length")),
        merkle_root: payload[40..72].try_into().expect("slice length"),
    };
    validate_scope_digest(&digest)?;
    Ok(digest)
}

fn validate_scope_digest(digest: &ScopeDigest) -> Result<(), ProtocolError> {
    if digest.scope_id == 0 {
        return Err(ProtocolError::new(
            ERROR_SCOPE_INVALID,
            "PIPESTREAM_SCOPE_INVALID",
            "root scope is not propagated with SCOPE_DIGEST",
        ));
    }
    if digest.entities_processed == 0 {
        return Err(ProtocolError::new(
            ERROR_SCOPE_INVALID,
            "PIPESTREAM_SCOPE_INVALID",
            "empty scope cannot be digested",
        ));
    }
    let classified = digest
        .entities_succeeded
        .checked_add(digest.entities_failed)
        .and_then(|value| value.checked_add(digest.entities_deferred))
        .ok_or_else(|| ProtocolError::limit("scope digest counters overflow"))?;
    if classified > digest.entities_processed {
        return Err(ProtocolError::entity(
            "scope digest classifications exceed processed count",
        ));
    }
    Ok(())
}

pub fn encode_barrier(barrier: Barrier) -> Result<Vec<u8>, ProtocolError> {
    validate_barrier(barrier)?;
    let mut payload = Vec::with_capacity(12);
    payload.extend_from_slice(&(u32::from(barrier.released) << 31).to_be_bytes());
    payload.extend_from_slice(&barrier.scope_id.to_be_bytes());
    payload.extend_from_slice(&barrier.parent_entity_id.to_be_bytes());
    encode_ucf(FRAME_BARRIER, &payload)
}

pub fn decode_barrier(payload: &[u8]) -> Result<Barrier, ProtocolError> {
    if payload.len() != 12 {
        return Err(ProtocolError::frame("invalid BARRIER payload length"));
    }
    let word = u32::from_be_bytes(payload[..4].try_into().expect("slice length"));
    let barrier = Barrier {
        released: word & (1 << 31) != 0,
        scope_id: u32::from_be_bytes(payload[4..8].try_into().expect("slice length")),
        parent_entity_id: u32::from_be_bytes(payload[8..12].try_into().expect("slice length")),
    };
    validate_barrier(barrier)?;
    Ok(barrier)
}

fn validate_barrier(barrier: Barrier) -> Result<(), ProtocolError> {
    if barrier.scope_id == 0 {
        return Err(ProtocolError::new(
            ERROR_SCOPE_INVALID,
            "PIPESTREAM_SCOPE_INVALID",
            "BARRIER requires a child scope",
        ));
    }
    if barrier.parent_entity_id == 0 || barrier.parent_entity_id > MAX_ENTITY_ID {
        return Err(ProtocolError::entity("BARRIER parent entity is reserved"));
    }
    Ok(())
}

pub fn encode_claim_redemption(redemption: &ClaimRedemption) -> Result<Vec<u8>, ProtocolError> {
    validate_claim_redemption(redemption)?;
    let mut body = Vec::new();
    let mut encoder = Encoder::new(&mut body);
    encoder.map(4).map_err(cbor_encode)?;
    encoder
        .str("flags")
        .map_err(cbor_encode)?
        .u8(u8::from(redemption.acknowledged))
        .map_err(cbor_encode)?;
    encoder
        .str("claim-id")
        .map_err(cbor_encode)?
        .u64(redemption.claim_id)
        .map_err(cbor_encode)?;
    encoder
        .str("session-id")
        .map_err(cbor_encode)?
        .str(&redemption.session_id)
        .map_err(cbor_encode)?;
    encoder
        .str("state-checksum")
        .map_err(cbor_encode)?
        .bytes(&redemption.state_checksum)
        .map_err(cbor_encode)?;
    encode_ucf(FRAME_CLAIM_REDEMPTION, &body)
}

pub fn decode_claim_redemption(payload: &[u8]) -> Result<ClaimRedemption, ProtocolError> {
    let mut decoder = Decoder::new(payload);
    let count = decoder
        .map()
        .map_err(cbor_decode)?
        .ok_or_else(|| ProtocolError::frame("indefinite claim redemption is not permitted"))?;
    let mut flags = None;
    let mut claim_id = None;
    let mut session_id = None;
    let mut state_checksum = None;
    for _ in 0..count {
        match decoder.str().map_err(cbor_decode)? {
            "flags" => flags = Some(decoder.u8().map_err(cbor_decode)?),
            "claim-id" => claim_id = Some(decoder.u64().map_err(cbor_decode)?),
            "session-id" => session_id = Some(decoder.str().map_err(cbor_decode)?.to_owned()),
            "state-checksum" => {
                state_checksum = Some(
                    decoder
                        .bytes()
                        .map_err(cbor_decode)?
                        .try_into()
                        .map_err(|_| {
                            ProtocolError::integrity("state checksum must be 32 octets")
                        })?,
                )
            }
            key => {
                return Err(ProtocolError::frame(format!(
                    "unknown claim redemption field {key}"
                )));
            }
        }
    }
    if decoder.position() != payload.len() {
        return Err(ProtocolError::frame(
            "trailing claim redemption CBOR octets",
        ));
    }
    let flags = flags.ok_or_else(|| ProtocolError::frame("claim flags are absent"))?;
    if flags > 1 {
        return Err(ProtocolError::frame("unknown claim redemption flags"));
    }
    let redemption = ClaimRedemption {
        session_id: session_id.ok_or_else(|| ProtocolError::entity("session-id is absent"))?,
        claim_id: claim_id.ok_or_else(|| ProtocolError::entity("claim-id is absent"))?,
        state_checksum: state_checksum
            .ok_or_else(|| ProtocolError::integrity("state checksum is absent"))?,
        acknowledged: flags == 1,
    };
    validate_claim_redemption(&redemption)?;
    let canonical = encode_claim_redemption(&redemption)?;
    if canonical[5..] != *payload {
        return Err(ProtocolError::frame(
            "claim redemption CBOR is not deterministic",
        ));
    }
    Ok(redemption)
}

fn validate_claim_redemption(redemption: &ClaimRedemption) -> Result<(), ProtocolError> {
    if redemption.claim_id == 0 {
        return Err(ProtocolError::new(
            ERROR_CLAIM_NOT_FOUND,
            "PIPESTREAM_CLAIM_NOT_FOUND",
            "claim ID is reserved",
        ));
    }
    if redemption.session_id.is_empty()
        || redemption.session_id.len() > 128
        || !redemption
            .session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(ProtocolError::entity("invalid session-id"));
    }
    Ok(())
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
    encode_entity_for(header, payload, LayerSupport::LAYER0)
}

pub fn encode_entity_for(
    header: &EntityHeader,
    payload: &[u8],
    layers: LayerSupport,
) -> Result<Vec<u8>, ProtocolError> {
    validate_entity_payload(header, payload.len() as u64, Sha256::digest(payload).into())?;
    let encoded = encode_entity_header_for(header, layers)?;
    let mut output = Vec::with_capacity(4 + encoded.len() + payload.len());
    output.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    output.extend_from_slice(&encoded);
    output.extend_from_slice(payload);
    Ok(output)
}

/// Encode only the CBOR header, without the four-octet length or payload.
pub fn encode_entity_header_for(
    header: &EntityHeader,
    layers: LayerSupport,
) -> Result<Vec<u8>, ProtocolError> {
    validate_entity_header_for(header, layers)?;
    let fields = 2
        + usize::from(header.parent_id.is_some())
        + usize::from(header.scope_id.is_some())
        + usize::from(header.parent_scope_id.is_some())
        + usize::from(header.content_type.is_some())
        + usize::from(header.payload_length.is_some())
        + usize::from(header.checksum.is_some())
        + usize::from(!header.metadata.is_empty())
        + usize::from(header.chunk_info.is_some())
        + usize::from(header.completion_policy.is_some());
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
    if !header.metadata.is_empty() {
        encoder.str("metadata").map_err(cbor_encode)?;
        encode_metadata(&mut encoder, &header.metadata)?;
    }
    if let Some(scope_id) = header.scope_id {
        encoder
            .str("scope-id")
            .map_err(cbor_encode)?
            .u32(scope_id)
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
    if let Some(chunk_info) = header.chunk_info {
        encoder.str("chunk-info").map_err(cbor_encode)?;
        encode_chunk_info(&mut encoder, chunk_info)?;
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
    if let Some(parent_scope_id) = header.parent_scope_id {
        encoder
            .str("parent-scope-id")
            .map_err(cbor_encode)?
            .u32(parent_scope_id)
            .map_err(cbor_encode)?;
    }
    if let Some(policy) = &header.completion_policy {
        encoder.str("completion-policy").map_err(cbor_encode)?;
        encode_completion_policy(&mut encoder, policy)?;
    }
    if encoded.len() > MAX_ENTITY_HEADER {
        return Err(ProtocolError::limit("entity header exceeds local limit"));
    }
    Ok(encoded)
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
            scope_id: None,
            parent_scope_id: None,
            layer: 0,
            content_type: Some(content_type.to_owned()),
            payload_length: Some(payload.len() as u64),
            checksum: Some(checksum),
            metadata: BTreeMap::new(),
            chunk_info: None,
            completion_policy: None,
        },
        payload,
    )
}

pub fn decode_entity(data: &[u8]) -> Result<(EntityHeader, &[u8]), ProtocolError> {
    decode_entity_for(data, LayerSupport::LAYER0)
}

pub fn decode_entity_for(
    data: &[u8],
    layers: LayerSupport,
) -> Result<(EntityHeader, &[u8]), ProtocolError> {
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
    let header = decode_entity_header_for(encoded, layers)?;
    validate_entity_payload(
        &header,
        payload.len() as u64,
        Sha256::digest(payload).into(),
    )?;
    Ok((header, payload))
}

/// Decode and validate a bounded CBOR header before reading any payload octets.
/// Call `validate_entity_payload` after reading to FIN and computing its digest.
pub fn decode_entity_header_for(
    encoded: &[u8],
    layers: LayerSupport,
) -> Result<EntityHeader, ProtocolError> {
    if encoded.len() > MAX_ENTITY_HEADER {
        return Err(ProtocolError::limit("entity header exceeds local limit"));
    }
    deterministic::validate(encoded)?;
    let mut decoder = Decoder::new(encoded);
    let count = decoder
        .map()
        .map_err(cbor_decode)?
        .ok_or_else(|| ProtocolError::frame("indefinite maps are not permitted"))?;
    let mut entity_id = None;
    let mut parent_id = None;
    let mut scope_id = None;
    let mut parent_scope_id = None;
    let mut layer = None;
    let mut content_type = None;
    let mut payload_length = None;
    let mut checksum = None;
    let mut metadata = BTreeMap::new();
    let mut chunk_info = None;
    let mut completion_policy = None;
    for _ in 0..count {
        let key = decoder.str().map_err(cbor_decode)?;
        match key {
            "entity-id" => entity_id = Some(decoder.u32().map_err(cbor_decode)?),
            "parent-id" => parent_id = Some(decoder.u32().map_err(cbor_decode)?),
            "scope-id" => scope_id = Some(decoder.u32().map_err(cbor_decode)?),
            "parent-scope-id" => parent_scope_id = Some(decoder.u32().map_err(cbor_decode)?),
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
            "metadata" => metadata = decode_metadata(&mut decoder)?,
            "chunk-info" => chunk_info = Some(decode_chunk_info(&mut decoder)?),
            "completion-policy" => {
                completion_policy = Some(decode_completion_policy(&mut decoder)?)
            }
            _ => {
                return Err(ProtocolError::frame(format!(
                    "unsupported entity field {key}"
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
        scope_id,
        parent_scope_id,
        layer: layer.ok_or_else(|| ProtocolError::entity("layer is absent"))?,
        content_type,
        payload_length,
        checksum,
        metadata,
        chunk_info,
        completion_policy,
    };
    validate_entity_header_for(&header, layers)?;
    Ok(header)
}

fn validate_entity_header_for(
    header: &EntityHeader,
    layers: LayerSupport,
) -> Result<(), ProtocolError> {
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
    if (header.scope_id.is_some() || header.parent_scope_id.is_some()) && !layers.layer1_recursive {
        return Err(ProtocolError::new(
            ERROR_LAYER_UNSUPPORTED,
            "PIPESTREAM_LAYER_UNSUPPORTED",
            "scope fields require Layer 1",
        ));
    }
    if header.completion_policy.is_some() && !layers.layer2_resilience {
        return Err(ProtocolError::new(
            ERROR_LAYER_UNSUPPORTED,
            "PIPESTREAM_LAYER_UNSUPPORTED",
            "completion policy requires Layer 2",
        ));
    }
    if let Some(scope_id) = header.scope_id {
        if scope_id == 0 && header.parent_scope_id.is_some() {
            return Err(ProtocolError::new(
                ERROR_SCOPE_INVALID,
                "PIPESTREAM_SCOPE_INVALID",
                "root scope cannot have a parent scope",
            ));
        }
        if scope_id != 0 && (header.parent_id.is_none() || header.parent_scope_id.is_none()) {
            return Err(ProtocolError::new(
                ERROR_SCOPE_INVALID,
                "PIPESTREAM_SCOPE_INVALID",
                "cross-scope parent requires parent-id and parent-scope-id",
            ));
        }
        if scope_id != 0 && header.parent_scope_id == Some(scope_id) {
            return Err(ProtocolError::new(
                ERROR_SCOPE_INVALID,
                "PIPESTREAM_SCOPE_INVALID",
                "child scope and parent scope must differ",
            ));
        }
    }
    if header.parent_scope_id.is_some() && header.scope_id.is_none() {
        return Err(ProtocolError::new(
            ERROR_SCOPE_INVALID,
            "PIPESTREAM_SCOPE_INVALID",
            "parent-scope-id requires scope-id",
        ));
    }
    if let Some(chunk) = header.chunk_info
        && (chunk.total_chunks == 0 || chunk.chunk_index >= chunk.total_chunks)
    {
        return Err(ProtocolError::entity("invalid chunk-info"));
    }
    if header.metadata.len() > 1_024
        || header
            .metadata
            .iter()
            .any(|(key, value)| key.len() > 4_096 || value.len() > 4_096)
    {
        return Err(ProtocolError::limit("entity metadata exceeds local limit"));
    }
    if let Some(policy) = &header.completion_policy {
        validate_completion_policy(policy)?;
    }
    Ok(())
}

/// Validate measured payload properties. A header alone is not an entity receipt.
pub fn validate_entity_payload(
    header: &EntityHeader,
    actual_length: u64,
    actual_checksum: [u8; 32],
) -> Result<(), ProtocolError> {
    if actual_length > MAX_PAYLOAD as u64 {
        return Err(ProtocolError::limit("entity payload exceeds local limit"));
    }
    if header
        .payload_length
        .is_some_and(|length| length != actual_length)
    {
        return Err(ProtocolError::entity("payload-length mismatch"));
    }
    if header
        .checksum
        .is_some_and(|expected| expected != actual_checksum)
    {
        return Err(ProtocolError::integrity("checksum mismatch"));
    }
    Ok(())
}

fn encode_metadata(
    encoder: &mut Encoder<&mut Vec<u8>>,
    metadata: &BTreeMap<String, String>,
) -> Result<(), ProtocolError> {
    encoder.map(metadata.len() as u64).map_err(cbor_encode)?;
    let mut entries: Vec<_> = metadata.iter().collect();
    entries.sort_by(|(left, _), (right, _)| {
        left.len()
            .cmp(&right.len())
            .then_with(|| left.as_bytes().cmp(right.as_bytes()))
    });
    for (key, value) in entries {
        encoder
            .str(key)
            .map_err(cbor_encode)?
            .str(value)
            .map_err(cbor_encode)?;
    }
    Ok(())
}

fn decode_metadata(decoder: &mut Decoder<'_>) -> Result<BTreeMap<String, String>, ProtocolError> {
    let count = decoder
        .map()
        .map_err(cbor_decode)?
        .ok_or_else(|| ProtocolError::frame("indefinite metadata maps are not permitted"))?;
    if count > 1_024 {
        return Err(ProtocolError::limit("entity metadata exceeds local limit"));
    }
    let mut metadata = BTreeMap::new();
    for _ in 0..count {
        let key = decoder.str().map_err(cbor_decode)?.to_owned();
        let value = decoder.str().map_err(cbor_decode)?.to_owned();
        if metadata.insert(key, value).is_some() {
            return Err(ProtocolError::frame("duplicate metadata key"));
        }
    }
    Ok(metadata)
}

fn encode_chunk_info(
    encoder: &mut Encoder<&mut Vec<u8>>,
    chunk: ChunkInfo,
) -> Result<(), ProtocolError> {
    encoder.map(3).map_err(cbor_encode)?;
    encoder
        .str("chunk-index")
        .map_err(cbor_encode)?
        .u64(chunk.chunk_index)
        .map_err(cbor_encode)?;
    encoder
        .str("chunk-offset")
        .map_err(cbor_encode)?
        .u64(chunk.chunk_offset)
        .map_err(cbor_encode)?;
    encoder
        .str("total-chunks")
        .map_err(cbor_encode)?
        .u64(chunk.total_chunks)
        .map_err(cbor_encode)?;
    Ok(())
}

fn decode_chunk_info(decoder: &mut Decoder<'_>) -> Result<ChunkInfo, ProtocolError> {
    let count = decoder
        .map()
        .map_err(cbor_decode)?
        .ok_or_else(|| ProtocolError::frame("indefinite chunk-info maps are not permitted"))?;
    let mut total_chunks = None;
    let mut chunk_index = None;
    let mut chunk_offset = None;
    for _ in 0..count {
        match decoder.str().map_err(cbor_decode)? {
            "total-chunks" => total_chunks = Some(decoder.u64().map_err(cbor_decode)?),
            "chunk-index" => chunk_index = Some(decoder.u64().map_err(cbor_decode)?),
            "chunk-offset" => chunk_offset = Some(decoder.u64().map_err(cbor_decode)?),
            key => {
                return Err(ProtocolError::frame(format!(
                    "unknown chunk-info field {key}"
                )));
            }
        }
    }
    Ok(ChunkInfo {
        total_chunks: total_chunks
            .ok_or_else(|| ProtocolError::entity("total-chunks is absent"))?,
        chunk_index: chunk_index.ok_or_else(|| ProtocolError::entity("chunk-index is absent"))?,
        chunk_offset: chunk_offset
            .ok_or_else(|| ProtocolError::entity("chunk-offset is absent"))?,
    })
}

fn encode_completion_policy(
    encoder: &mut Encoder<&mut Vec<u8>>,
    policy: &CompletionPolicy,
) -> Result<(), ProtocolError> {
    let fields = usize::from(policy.mode.is_some())
        + usize::from(policy.max_retries.is_some())
        + usize::from(policy.retry_delay_ms.is_some())
        + usize::from(policy.timeout_ms.is_some())
        + usize::from(policy.min_success_ratio.is_some())
        + usize::from(policy.on_timeout.is_some())
        + usize::from(policy.on_failure.is_some());
    encoder.map(fields as u64).map_err(cbor_encode)?;
    if let Some(mode) = policy.mode {
        encoder
            .str("mode")
            .map_err(cbor_encode)?
            .u8(mode as u8)
            .map_err(cbor_encode)?;
    }
    if let Some(action) = policy.on_failure {
        encoder
            .str("on-failure")
            .map_err(cbor_encode)?
            .u8(action as u8)
            .map_err(cbor_encode)?;
    }
    if let Some(action) = policy.on_timeout {
        encoder
            .str("on-timeout")
            .map_err(cbor_encode)?
            .u8(action as u8)
            .map_err(cbor_encode)?;
    }
    if let Some(timeout_ms) = policy.timeout_ms {
        encoder
            .str("timeout-ms")
            .map_err(cbor_encode)?
            .u64(timeout_ms)
            .map_err(cbor_encode)?;
    }
    if let Some(max_retries) = policy.max_retries {
        encoder
            .str("max-retries")
            .map_err(cbor_encode)?
            .u64(max_retries)
            .map_err(cbor_encode)?;
    }
    if let Some(retry_delay_ms) = policy.retry_delay_ms {
        encoder
            .str("retry-delay-ms")
            .map_err(cbor_encode)?
            .u64(retry_delay_ms)
            .map_err(cbor_encode)?;
    }
    if let Some(ratio) = policy.min_success_ratio {
        encoder.str("min-success-ratio").map_err(cbor_encode)?;
        if deterministic::fits_f16(ratio) {
            encoder.f16(ratio).map_err(cbor_encode)?;
        } else {
            encoder.f32(ratio).map_err(cbor_encode)?;
        }
    }
    Ok(())
}

fn decode_completion_policy(decoder: &mut Decoder<'_>) -> Result<CompletionPolicy, ProtocolError> {
    let count = decoder
        .map()
        .map_err(cbor_decode)?
        .ok_or_else(|| ProtocolError::frame("indefinite completion-policy is not permitted"))?;
    let mut policy = CompletionPolicy::default();
    for _ in 0..count {
        match decoder.str().map_err(cbor_decode)? {
            "mode" => policy.mode = Some(completion_mode(decoder.u8().map_err(cbor_decode)?)?),
            "max-retries" => policy.max_retries = Some(decoder.u64().map_err(cbor_decode)?),
            "retry-delay-ms" => policy.retry_delay_ms = Some(decoder.u64().map_err(cbor_decode)?),
            "timeout-ms" => policy.timeout_ms = Some(decoder.u64().map_err(cbor_decode)?),
            "min-success-ratio" => {
                policy.min_success_ratio = Some(decoder.f32().map_err(cbor_decode)?)
            }
            "on-timeout" => {
                policy.on_timeout = Some(failure_action(decoder.u8().map_err(cbor_decode)?)?)
            }
            "on-failure" => {
                policy.on_failure = Some(failure_action(decoder.u8().map_err(cbor_decode)?)?)
            }
            key => {
                return Err(ProtocolError::frame(format!(
                    "unknown completion-policy field {key}"
                )));
            }
        }
    }
    validate_completion_policy(&policy)?;
    Ok(policy)
}

fn completion_mode(value: u8) -> Result<CompletionMode, ProtocolError> {
    match value {
        0 => Ok(CompletionMode::Unspecified),
        1 => Ok(CompletionMode::Strict),
        2 => Ok(CompletionMode::Lenient),
        3 => Ok(CompletionMode::BestEffort),
        4 => Ok(CompletionMode::Quorum),
        _ => Err(ProtocolError::entity("invalid completion mode")),
    }
}

fn failure_action(value: u8) -> Result<FailureAction, ProtocolError> {
    match value {
        0 => Ok(FailureAction::Unspecified),
        1 => Ok(FailureAction::Fail),
        2 => Ok(FailureAction::Skip),
        3 => Ok(FailureAction::Retry),
        4 => Ok(FailureAction::Defer),
        _ => Err(ProtocolError::entity("invalid failure action")),
    }
}

fn validate_completion_policy(policy: &CompletionPolicy) -> Result<(), ProtocolError> {
    if let Some(ratio) = policy.min_success_ratio
        && (!ratio.is_finite() || !(0.0..=1.0).contains(&ratio))
    {
        return Err(ProtocolError::entity(
            "min-success-ratio must be finite and between zero and one",
        ));
    }
    if policy.effective_mode() == CompletionMode::Quorum && policy.min_success_ratio.is_none() {
        return Err(ProtocolError::entity(
            "quorum completion requires min-success-ratio",
        ));
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

    #[test]
    fn optional_fields_preserve_received_representation() {
        for row in include_str!("../../../test-vectors/optional-fields.tsv")
            .lines()
            .skip(1)
        {
            let fields: Vec<_> = row.split('\t').collect();
            let bytes: Vec<_> = fields[3]
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            let result = match fields[1] {
                "capabilities" => decode_capabilities(&bytes).map(|_| ()),
                "checkpoint" => decode_checkpoint(&bytes).map(|_| ()),
                _ => panic!("unknown vector kind"),
            };
            assert_eq!(
                result.is_ok(),
                fields[2] == "valid",
                "{}: {result:?}",
                fields[0]
            );
            if let Err(error) = result {
                assert_eq!(error.name, "PIPESTREAM_FRAME_ERROR");
            }
        }
    }
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
    fn recursive_capabilities_round_trip_deterministically() {
        let capabilities = Capabilities {
            layer0_core: true,
            layer1_recursive: true,
            layer2_resilience: true,
            max_scope_depth: Some(5),
            max_entities_per_scope: Some(10_000),
            max_window_size: 4_096,
            serialization_format: 0,
            keepalive_timeout_ms: 15_000,
            extensions: extensions::Extensions::default(),
        };
        let encoded = encode_capabilities(&capabilities).unwrap();
        let (frame_type, payload) = decode_ucf(&encoded).unwrap();
        assert_eq!(FRAME_CAPABILITIES, frame_type);
        assert_eq!(capabilities, decode_capabilities(payload).unwrap());
    }

    #[test]
    fn scoped_entity_round_trip_covers_full_header() {
        let mut metadata = BTreeMap::new();
        metadata.insert("kind".to_owned(), "video-segment".to_owned());
        metadata.insert("codec".to_owned(), "av1".to_owned());
        let payload = b"recursive child";
        let header = EntityHeader {
            entity_id: 2,
            parent_id: Some(1),
            scope_id: Some(7),
            parent_scope_id: Some(0),
            layer: 1,
            content_type: Some("application/octet-stream".to_owned()),
            payload_length: Some(payload.len() as u64),
            checksum: Some(Sha256::digest(payload).into()),
            metadata,
            chunk_info: Some(ChunkInfo {
                total_chunks: 3,
                chunk_index: 1,
                chunk_offset: 128,
            }),
            completion_policy: Some(CompletionPolicy {
                mode: Some(CompletionMode::Quorum),
                max_retries: Some(2),
                retry_delay_ms: Some(250),
                timeout_ms: Some(30_000),
                min_success_ratio: Some(0.75),
                on_timeout: Some(FailureAction::Defer),
                on_failure: Some(FailureAction::Retry),
            }),
        };
        let encoded = encode_entity_for(&header, payload, LayerSupport::LAYER2).unwrap();
        let (decoded, body) = decode_entity_for(&encoded, LayerSupport::LAYER2).unwrap();
        assert_eq!(header, decoded);
        assert_eq!(payload, body);
        assert_eq!(
            ERROR_LAYER_UNSUPPORTED,
            decode_entity(&encoded).unwrap_err().code
        );
    }

    #[test]
    fn recursive_and_resilience_statuses_round_trip() {
        let scoped = StatusFrame {
            status: Status {
                state: STATUS_PROCESSING,
                entity_id: 9,
                scope_id: 7,
                cursor: None,
                depth: 2,
            },
            extension: None,
        };
        let encoded = encode_status_frame(&scoped, LayerSupport::LAYER1).unwrap();
        let (_, payload) = decode_ucf(&encoded).unwrap();
        assert_eq!(
            scoped,
            decode_status_frame(payload, LayerSupport::LAYER1).unwrap()
        );
        assert_eq!(
            ERROR_LAYER_UNSUPPORTED,
            decode_status(payload).unwrap_err().code
        );

        let yielded = StatusFrame {
            status: Status {
                state: STATUS_YIELDED,
                entity_id: 9,
                scope_id: 7,
                cursor: None,
                depth: 2,
            },
            extension: Some(StatusExtension::Yield {
                reason: 1,
                token: b"opaque continuation".to_vec(),
            }),
        };
        let encoded = encode_status_frame(&yielded, LayerSupport::LAYER2).unwrap();
        let (_, payload) = decode_ucf(&encoded).unwrap();
        assert_eq!(
            yielded,
            decode_status_frame(payload, LayerSupport::LAYER2).unwrap()
        );
    }

    #[test]
    fn scope_digest_barrier_and_claim_redemption_round_trip() {
        let digest = ScopeDigest {
            scope_id: 7,
            entities_processed: 3,
            entities_succeeded: 2,
            entities_failed: 1,
            entities_deferred: 0,
            merkle_root: [0x5a; 32],
        };
        let encoded = encode_scope_digest(&digest).unwrap();
        let (frame_type, payload) = decode_ucf(&encoded).unwrap();
        assert_eq!(FRAME_SCOPE_DIGEST, frame_type);
        assert_eq!(digest, decode_scope_digest(payload).unwrap());

        let barrier = Barrier {
            released: true,
            scope_id: 7,
            parent_entity_id: 1,
        };
        let encoded = encode_barrier(barrier).unwrap();
        let (frame_type, payload) = decode_ucf(&encoded).unwrap();
        assert_eq!(FRAME_BARRIER, frame_type);
        assert_eq!(barrier, decode_barrier(payload).unwrap());

        let claim = ClaimRedemption {
            session_id: "durable-session-1".to_owned(),
            claim_id: 42,
            state_checksum: [0xa5; 32],
            acknowledged: false,
        };
        let encoded = encode_claim_redemption(&claim).unwrap();
        let (frame_type, payload) = decode_ucf(&encoded).unwrap();
        assert_eq!(FRAME_CLAIM_REDEMPTION, frame_type);
        assert_eq!(claim, decode_claim_redemption(payload).unwrap());
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

    #[test]
    fn recursive_corpus_has_exact_encodings_and_named_refusals() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../test-vectors/recursive/index.tsv");
        let index = fs::read_to_string(path).unwrap();
        let mut lines = index.lines();
        assert_eq!(
            Some("name\tkind\tlayer\texpectation\terror\thex"),
            lines.next()
        );
        for row in lines {
            let fields = row.split('\t').collect::<Vec<_>>();
            assert_eq!(6, fields.len(), "malformed recursive vector row: {row}");
            let name = fields[0];
            let kind = fields[1];
            let layers = match fields[2] {
                "0" => LayerSupport::LAYER0,
                "1" => LayerSupport::LAYER1,
                "2" => LayerSupport::LAYER2,
                other => panic!("{name}: invalid layer {other}"),
            };
            let bytes = decode_hex_fixture(fields[5]);
            let result = decode_recursive_vector(kind, layers, &bytes);
            if fields[3] == "valid" {
                assert_eq!(
                    bytes,
                    result.unwrap_or_else(|error| panic!("{name}: {error}"))
                );
            } else {
                assert_eq!(fields[4], result.unwrap_err().name, "{name}");
            }
        }
    }

    fn decode_recursive_vector(
        kind: &str,
        layers: LayerSupport,
        bytes: &[u8],
    ) -> Result<Vec<u8>, ProtocolError> {
        if kind == "entity" {
            let (header, payload) = decode_entity_for(bytes, layers)?;
            return encode_entity_for(&header, payload, layers);
        }
        let (frame_type, payload) = decode_ucf(bytes)?;
        match kind {
            "status" => {
                if frame_type != FRAME_STATUS {
                    return Err(ProtocolError::frame("vector frame is not STATUS"));
                }
                encode_status_frame(&decode_status_frame(payload, layers)?, layers)
            }
            "scope-digest" => {
                if frame_type != FRAME_SCOPE_DIGEST {
                    return Err(ProtocolError::frame("vector frame is not SCOPE_DIGEST"));
                }
                encode_scope_digest(&decode_scope_digest(payload)?)
            }
            "barrier" => {
                if frame_type != FRAME_BARRIER {
                    return Err(ProtocolError::frame("vector frame is not BARRIER"));
                }
                encode_barrier(decode_barrier(payload)?)
            }
            "checkpoint" => {
                if frame_type != FRAME_CHECKPOINT {
                    return Err(ProtocolError::frame("vector frame is not CHECKPOINT"));
                }
                encode_checkpoint_for(&decode_checkpoint_for(payload, layers)?, layers)
            }
            "claim-redemption" => {
                if frame_type != FRAME_CLAIM_REDEMPTION {
                    return Err(ProtocolError::frame("vector frame is not CLAIM_REDEMPTION"));
                }
                encode_claim_redemption(&decode_claim_redemption(payload)?)
            }
            _ => Err(ProtocolError::frame("unknown recursive vector kind")),
        }
    }

    fn decode_hex_fixture(value: &str) -> Vec<u8> {
        assert_eq!(0, value.len() % 2);
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16)
                    .expect("recursive vector contains hexadecimal octets")
            })
            .collect()
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
