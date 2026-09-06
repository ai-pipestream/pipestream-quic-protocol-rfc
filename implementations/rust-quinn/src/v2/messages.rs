use super::{codec::*, records::scope_identity, *};

message!(Session {
    0 => Create { request: Id, creation_sequence: Id, policy: Policy },
    1 => Binding { request: Id, authority: IdentityLabel, owner: IdentityLabel, generation: Id, creation_sequence: Id, policy: Policy, limits: Limits },
    2 => Attach { request: Id, authority: IdentityLabel, owner: IdentityLabel, generation: Id },
    3 => NextSequence { request: Id },
    4 => Sequence { request: Id, next_creation_sequence: Id },
});
impl Session {
    fn check_fields(&self) -> Result<(), Error> {
        Ok(())
    }
}

message!(Scope {
    0 => Declare { request: Id, operation: OperationId, scope: Number, entity_ids: Vec<Id>, seal: bool },
    1 => Declared { request: Id, receipt: OperationReceipt },
    2 => Page { request: Id, scope: Number, after_entity: Number, limit: PageLimit },
    3 => PageResponse { request: Id, scope: Number, producer: Producer, parent: Option<WorkKey>, sealed: bool, seal: Option<Digest>, declared: Number, entries: Vec<ScopeEntry>, more: bool },
    4 => Checkpoint { request: Id, scope: Number, seal: Digest, wait_ms: WaitMs },
    5 => CheckpointResponse { request: Id, summary: ScopeSummary },
    6 => Cancel { request: Id, operation: OperationId, scope: Number },
    7 => Cancelled { request: Id, receipt: OperationReceipt },
});
impl Scope {
    fn check_fields(&self) -> Result<(), Error> {
        match self {
            Self::Declare {
                entity_ids, seal, ..
            } => {
                require(
                    !entity_ids.is_empty() || *seal,
                    "empty unsealed declaration",
                )?;
                require(
                    entity_ids.windows(2).all(|p| p[0] < p[1]),
                    "declaration IDs not increasing",
                )
            }
            Self::Declared { receipt, .. } => receipt_kind(receipt, 1),
            Self::Cancelled { receipt, .. } => receipt_kind(receipt, 4),
            Self::PageResponse {
                scope,
                producer,
                parent,
                sealed,
                seal,
                declared,
                entries,
                more,
                ..
            } => {
                scope_identity(*scope, *producer, parent.as_ref())?;
                require(*sealed == seal.is_some(), "page seal flag mismatch")?;
                require(
                    entries.len() as u64 <= declared.0
                        && entries.windows(2).all(|p| p[0].entity < p[1].entity),
                    "invalid scope page entries",
                )?;
                require(
                    !*more || (!entries.is_empty() && (entries.len() as u64) < declared.0),
                    "invalid page continuation",
                )
            }
            _ => Ok(()),
        }
    }
}

fn receipt_kind(receipt: &OperationReceipt, kind: u8) -> Result<(), Error> {
    require(receipt.body.kind() == kind, "wrong typed receipt outcome")
}

message!(Work {
    1 => Admitted { request: RequestTag, receipt: OperationReceipt },
    2 => Operation { request: Id, operation: OperationId },
    3 => OperationResponse { request: Id, receipt: OperationReceipt },
    4 => Watch { request: Id, work: WorkKey, after_revision: Number, wait_ms: WaitMs },
    5 => View { request: Id, revision: Id, work: Box<WorkView> },
    6 => Retry { request: Id, operation: OperationId, work: WorkKey, expected_attempt: Id },
    7 => Retried { request: Id, receipt: OperationReceipt },
    8 => Cancel { request: Id, operation: OperationId, work: WorkKey },
    9 => Cancelled { request: Id, receipt: OperationReceipt },
    10 => Skip { request: Id, operation: OperationId, work: WorkKey },
    11 => Skipped { request: Id, receipt: OperationReceipt },
});
impl Work {
    fn check_fields(&self) -> Result<(), Error> {
        match self {
            Self::Admitted { request, receipt } => {
                require(
                    matches!(request, RequestTag::Input { .. }),
                    "admission response requires input tag",
                )?;
                receipt_kind(receipt, 0)
            }
            Self::Retried { receipt, .. } => receipt_kind(receipt, 2),
            Self::Cancelled { receipt, .. } => receipt_kind(receipt, 3),
            Self::Skipped { receipt, .. } => receipt_kind(receipt, 5),
            _ => Ok(()),
        }
    }
}

message!(ResultMessage {
    0 => Read { request: Id, work: WorkKey, attempt: Id, index: OutputIndex, expected_sha256: Digest },
    1 => GetManifest { request: Id, work: WorkKey, attempt: Id },
    2 => ManifestResponse { request: Id, manifest: Manifest },
});
impl ResultMessage {
    fn check_fields(&self) -> Result<(), Error> {
        Ok(())
    }
}

message!(Drain {
    0 => Complete { request: Id, generation: Id, root_summary: ScopeSummary },
    1 => Completed { request: Id, generation: Id, root_summary: ScopeSummary },
    2 => Detach { request: Id },
    3 => Detached { request: Id },
});
impl Drain {
    fn check_fields(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    Capabilities(Capabilities),
    Session(Session),
    Scope(Scope),
    Work(Work),
    Result(ResultMessage),
    Drain(Drain),
    Refusal(Refusal),
    /// Opaque bounded bytes, not parsed CBOR or an activated extension.
    Ignorable {
        frame_type: u8,
        body: Vec<u8>,
    },
}

impl Control {
    pub fn frame_type(&self) -> u8 {
        match self {
            Self::Capabilities(_) => 1,
            Self::Session(_) => 2,
            Self::Scope(_) => 3,
            Self::Work(_) => 4,
            Self::Result(_) => 5,
            Self::Drain(_) => 6,
            Self::Refusal(_) => 7,
            Self::Ignorable { frame_type, .. } => *frame_type,
        }
    }

    /// Decode exactly one frame. The caller supplies the negotiated body limit;
    /// a CAPABILITIES frame always retains the initial 4096-byte bound.
    pub fn decode(frame: &[u8], limit: usize) -> Result<Self, Error> {
        require(frame.len() >= 5, "truncated control envelope")?;
        let length = control_body_length(frame[..5].try_into().expect("five bytes"), Some(limit))?;
        require(
            length <= limit && frame.len() - 5 == length,
            "control length mismatch or bound",
        )?;
        let body = &frame[5..];
        Ok(match frame[0] {
            1 => Self::Capabilities(decode(body, INITIAL_CONTROL_LIMIT)?),
            2 => Self::Session(decode(body, limit)?),
            3 => Self::Scope(decode(body, limit)?),
            4 => Self::Work(decode(body, limit)?),
            5 => Self::Result(decode(body, limit)?),
            6 => Self::Drain(decode(body, limit)?),
            7 => Self::Refusal(decode(body, limit)?),
            0x80..=0xbf => Self::Ignorable {
                frame_type: frame[0],
                body: body.to_vec(),
            },
            0xc0..=0xff => {
                return Err(Error::new(
                    ErrorCode::ExtensionUnsupported,
                    "private frame profile not activated",
                ));
            }
            _ => return Err(Error::frame("unknown required control type")),
        })
    }

    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, Error> {
        require(
            (INITIAL_CONTROL_LIMIT..=MAX_CONTROL_LIMIT).contains(&limit),
            "invalid control limit",
        )?;
        let body = match self {
            Self::Capabilities(v) => encode(v, INITIAL_CONTROL_LIMIT)?,
            Self::Session(v) => encode(v, limit)?,
            Self::Scope(v) => encode(v, limit)?,
            Self::Work(v) => encode(v, limit)?,
            Self::Result(v) => encode(v, limit)?,
            Self::Drain(v) => encode(v, limit)?,
            Self::Refusal(v) => encode(v, limit)?,
            Self::Ignorable { frame_type, body } => {
                require(
                    (0x80..=0xbf).contains(frame_type) && body.len() <= limit,
                    "invalid ignorable frame",
                )?;
                body.clone()
            }
        };
        let mut frame = Vec::with_capacity(body.len() + 5);
        frame.push(self.frame_type());
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&body);
        Ok(frame)
    }

    /// Check profile and sender direction. Exchange state and correlation are
    /// separate from the body codec; this method never changes durable work.
    pub fn validate_context(
        &self,
        from_client: bool,
        selected: Option<&Capabilities>,
    ) -> Result<(), Error> {
        if selected.is_none() {
            if let Self::Capabilities(c) = self {
                c.check()?;
            }
            return require(
                matches!(self, Self::Capabilities(c) if (c.response.0 == 0) == from_client),
                "only capabilities permitted before negotiation",
            );
        }
        let selected = selected.expect("checked above");
        selected.check()?;
        require(
            selected.response.0 == 1,
            "selected capabilities must be a response",
        )?;
        require(
            !matches!(self, Self::Capabilities(_)),
            "duplicate capabilities",
        )?;
        let (client_message, needs_durable, needs_results) = match self {
            Self::Capabilities(_) => unreachable!(),
            Self::Session(s) => (
                matches!(
                    s,
                    Session::Create { .. } | Session::Attach { .. } | Session::NextSequence { .. }
                ),
                true,
                false,
            ),
            Self::Scope(s) => (
                matches!(
                    s,
                    Scope::Declare { .. }
                        | Scope::Page { .. }
                        | Scope::Checkpoint { .. }
                        | Scope::Cancel { .. }
                ),
                true,
                false,
            ),
            Self::Work(w) => (
                matches!(
                    w,
                    Work::Operation { .. }
                        | Work::Watch { .. }
                        | Work::Retry { .. }
                        | Work::Cancel { .. }
                        | Work::Skip { .. }
                ),
                true,
                false,
            ),
            Self::Result(r) => (
                !matches!(r, ResultMessage::ManifestResponse { .. }),
                true,
                true,
            ),
            Self::Drain(d) => (
                matches!(d, Drain::Complete { .. } | Drain::Detach { .. }),
                matches!(d, Drain::Complete { .. } | Drain::Completed { .. }),
                false,
            ),
            Self::Refusal(_) => (false, false, false),
            Self::Ignorable { .. } => return Ok(()),
        };
        require(
            client_message == from_client,
            "wrong control message direction",
        )?;
        if (needs_durable && !selected.has(DURABLE_WORK))
            || (needs_results && !selected.has(RESULT_DELIVERY))
        {
            return Err(Error::new(
                ErrorCode::ExtensionUnsupported,
                "message profile not selected",
            ));
        }
        if let Self::Work(Work::View { work, .. }) = self {
            work.validate_profiles(selected.has(RESULT_DELIVERY))?;
        }
        Ok(())
    }
}

/// Inspect a control prefix before allocating its body. A transport supplies
/// None before negotiation and the selected limit thereafter. Body semantics
/// and private-type activation still require the normal frame/context checks.
pub fn control_body_length(
    prefix: [u8; 5],
    negotiated_limit: Option<usize>,
) -> Result<usize, Error> {
    if negotiated_limit.is_none() {
        require(
            prefix[0] == 1,
            "only capabilities permitted before negotiation",
        )?;
    }
    let limit = negotiated_limit.unwrap_or(INITIAL_CONTROL_LIMIT);
    require(
        (INITIAL_CONTROL_LIMIT..=MAX_CONTROL_LIMIT).contains(&limit),
        "invalid control limit",
    )?;
    let limit = if prefix[0] == 1 {
        INITIAL_CONTROL_LIMIT
    } else {
        limit
    };
    let length = u32::from_be_bytes(prefix[1..].try_into().expect("four bytes")) as usize;
    require(length <= limit, "control body exceeds bound")?;
    Ok(length)
}

/// Inspect the four-byte object header length before allocating a receive buffer.
pub fn object_header_length(prefix: [u8; 4]) -> Result<usize, Error> {
    let length = u32::from_be_bytes(prefix) as usize;
    require(
        (1..=MAX_HEADER).contains(&length),
        "object header length out of range",
    )?;
    Ok(length)
}

macro_rules! object_framing {
    ($ty:ident) => {
        impl $ty {
            pub fn decode_framed(header: &[u8]) -> Result<Self, Error> {
                require(header.len() >= 4, "truncated header length")?;
                let length = object_header_length(header[..4].try_into().expect("four bytes"))?;
                require(header.len() - 4 == length, "header length mismatch")?;
                Self::decode(&header[4..])
            }
            pub fn encode_framed(&self) -> Result<Vec<u8>, Error> {
                let bytes = self.encode()?;
                let mut header = Vec::with_capacity(4 + bytes.len());
                header.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                header.extend_from_slice(&bytes);
                Ok(header)
            }
        }
    };
}
object_framing!(InputHeader);
object_framing!(ResultHeader);
