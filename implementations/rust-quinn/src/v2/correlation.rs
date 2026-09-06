//! Bounded connection-local correlation. No transport event in this module
//! grants an attempt or changes a durable work outcome.

use super::{codec::Wire, *};
use std::{collections::BTreeMap, sync::Arc, time::Instant};

/// A particular response stream, bound to the book that accepted its header.
/// Moving it to an I/O task does not borrow or block the control book.
pub struct ResultTransfer {
    book: Arc<()>,
    request: Id,
    receiver: PayloadReceiver,
}

pub struct VerifiedResult {
    book: Arc<()>,
    request: Id,
    payload: VerifiedPayload,
}

impl ResultTransfer {
    pub fn receive(&mut self, chunk: &[u8], now: Instant) -> Result<(), Error> {
        self.receiver.receive(chunk, now)
    }
    pub fn check_deadline(&mut self, now: Instant) -> Result<(), Error> {
        self.receiver.check_deadline(now)
    }
    pub fn finish(mut self, now: Instant) -> Result<VerifiedResult, Error> {
        let payload = self.receiver.finish(now)?;
        Ok(VerifiedResult {
            book: self.book,
            request: self.request,
            payload,
        })
    }
}

#[derive(Debug)]
struct Pending {
    request: Control,
    result: Option<ResultHeader>,
    receiving_result: bool,
}

/// A client book records requests before transmission and accepts responses in
/// any order. It retains no unbounded completed-ID history. A fresh connection
/// needs a fresh book, not reused request numbers in the old one.
pub struct Correlation {
    identity: Arc<()>,
    selected: Capabilities,
    highest: u64,
    controls: BTreeMap<u64, Pending>,
    inputs: BTreeMap<u64, InputHeader>,
    highest_input: Option<u64>,
}

impl Correlation {
    pub fn new(selected: Capabilities) -> Result<Self, Error> {
        selected.check()?;
        require(
            selected.response.0 == 1,
            "correlation requires negotiated selection",
        )?;
        Ok(Self {
            identity: Arc::new(()),
            selected,
            highest: 0,
            controls: BTreeMap::new(),
            inputs: BTreeMap::new(),
            highest_input: None,
        })
    }

    pub fn pending(&self) -> usize {
        self.controls.len() + self.inputs.len()
    }

    fn capacity(&self) -> Result<(), Error> {
        if self.pending() as u64 >= self.selected.pending_limit.0 {
            Err(Error::new(
                ErrorCode::LimitExceeded,
                "pending request limit reached",
            ))
        } else {
            Ok(())
        }
    }

    /// A RESULT read must supply the previously authenticated manifest. The
    /// book compares its object commitment but does not authenticate that record.
    pub fn register(
        &mut self,
        request: &Control,
        manifest: Option<&Manifest>,
    ) -> Result<(), Error> {
        request.validate_context(true, Some(&self.selected))?;
        // Validate public constructed fields too, not only decoded requests.
        request.encode(self.selected.control_limit.0 as usize)?;
        let id =
            request_id(request).ok_or_else(|| Error::frame("not a correlated control request"))?;
        require(
            (self.highest != 0 || id.0 == 1) && id.0 > self.highest,
            "request ID not increasing from one",
        )?;
        self.capacity()?;
        let result = if let Control::Result(ResultMessage::Read {
            request,
            work,
            attempt,
            index,
            expected_sha256,
        }) = request
        {
            let manifest =
                manifest.ok_or_else(|| Error::frame("result read needs retained manifest"))?;
            manifest.check()?;
            let output = manifest
                .outputs
                .get(index.0 as usize)
                .ok_or_else(|| Error::frame("result index absent from manifest"))?;
            require(
                manifest.work == *work
                    && manifest.attempt == *attempt
                    && output.sha256 == *expected_sha256,
                "result request disagrees with manifest",
            )?;
            if output.length.0 > self.selected.object_limit.0 {
                return Err(Error::new(
                    ErrorCode::LimitExceeded,
                    "result exceeds object limit",
                ));
            }
            Some(ResultHeader {
                kind: Literal,
                request: *request,
                generation: manifest.generation,
                work: work.clone(),
                attempt: *attempt,
                index: *index,
                length: output.length,
                sha256: output.sha256,
            })
        } else {
            require(
                manifest.is_none(),
                "manifest supplied for a non-object request",
            )?;
            None
        };
        self.controls.insert(
            id.0,
            Pending {
                request: request.clone(),
                result,
                receiving_result: false,
            },
        );
        self.highest = id.0;
        Ok(())
    }

    /// Register actual locally allocated QUIC input IDs, in allocation order
    /// (not network arrival order). High water prevents stream-ID reuse.
    pub fn register_input(&mut self, stream: StreamId, header: &InputHeader) -> Result<(), Error> {
        RequestTag::Input { stream }.check()?;
        header.check()?;
        if !self.selected.has(DURABLE_WORK) {
            return Err(Error::new(
                ErrorCode::ExtensionUnsupported,
                "input requires durable profile",
            ));
        }
        require(
            self.highest_input.is_none_or(|old| stream.0 > old),
            "input stream ID reused",
        )?;
        self.capacity()?;
        if self.inputs.len() as u64 >= self.selected.stream_limit.0 {
            return Err(Error::new(
                ErrorCode::LimitExceeded,
                "incoming admission limit reached",
            ));
        }
        self.inputs.insert(stream.0, header.clone());
        self.highest_input = Some(stream.0);
        Ok(())
    }

    /// A control response completes its request only after shape/kind/known
    /// identity checks. Callers must still verify receipts' operation digests and
    /// retained session fields before accepting their durable claims.
    pub fn accept(&mut self, response: &Control) -> Result<(), Error> {
        response.validate_context(false, Some(&self.selected))?;
        response.encode(self.selected.control_limit.0 as usize)?;
        match response {
            Control::Work(Work::Admitted {
                request: RequestTag::Input { stream },
                receipt,
            }) => {
                let input = self
                    .inputs
                    .get(&stream.0)
                    .ok_or_else(|| Error::frame("unsolicited admission response"))?;
                require(
                    input.operation == receipt.operation,
                    "admission operation mismatch",
                )?;
                require(
                    matches!(&receipt.body, Outcome::Admitted { work, admitted_at, deadline, child, .. }
                    if work == &input.parameters.work && deadline.0 - admitted_at.0 == input.parameters.execution_ms.0
                    && match input.parameters.mode.0 {
                        0 => child.is_none(),
                        1 => child.as_ref().is_some_and(|c| c.producer.0 == 0),
                        2 => child.as_ref().is_some_and(|c| c.producer.0 == 1),
                        _ => false,
                    }),
                    "admission receipt disagrees with input parameters",
                )?;
                self.inputs.remove(&stream.0);
                return Ok(());
            }
            Control::Refusal(Refusal {
                request: RequestTag::Input { stream },
                ..
            }) => {
                require(
                    self.inputs.remove(&stream.0).is_some(),
                    "unsolicited input refusal",
                )?;
                return Ok(());
            }
            _ => {}
        }
        let id = request_id(response)
            .ok_or_else(|| Error::frame("not a correlated control response"))?;
        let pending = self
            .controls
            .get(&id.0)
            .ok_or_else(|| Error::frame("unsolicited or duplicate response"))?;
        require(
            !pending.receiving_result,
            "second control response after result header",
        )?;
        validate_reply(&pending.request, response)?;
        self.controls.remove(&id.0);
        Ok(())
    }

    /// The header starts delivery, not request completion. The returned receiver
    /// can validate incrementally without occupying the control reader.
    pub fn start_result(
        &mut self,
        header: &ResultHeader,
        now: Instant,
    ) -> Result<ResultTransfer, Error> {
        header.check()?;
        let pending = self
            .controls
            .get(&header.request.0)
            .ok_or_else(|| Error::frame("unsolicited result stream"))?;
        require(
            !pending.receiving_result && pending.result.as_ref() == Some(header),
            "duplicate or mismatched result header",
        )?;
        if self
            .controls
            .values()
            .filter(|p| p.receiving_result)
            .count() as u64
            >= self.selected.stream_limit.0
        {
            return Err(Error::new(
                ErrorCode::LimitExceeded,
                "result stream limit reached",
            ));
        }
        let pending = self
            .controls
            .get_mut(&header.request.0)
            .ok_or_else(|| Error::frame("unsolicited result stream"))?;
        let receiver = PayloadReceiver::new(header.length, header.sha256, &self.selected, now)?;
        pending.receiving_result = true;
        Ok(ResultTransfer {
            book: Arc::clone(&self.identity),
            request: header.request,
            receiver,
        })
    }

    pub fn finish_result(&mut self, verified: VerifiedResult) -> Result<(), Error> {
        require(
            Arc::ptr_eq(&self.identity, &verified.book),
            "result belongs to another connection",
        )?;
        let request = verified.request;
        let proof = &verified.payload;
        let pending = self
            .controls
            .get(&request.0)
            .ok_or_else(|| Error::frame("unknown result completion"))?;
        require(
            pending.receiving_result
                && pending
                    .result
                    .as_ref()
                    .is_some_and(|h| h.length == proof.length() && h.sha256 == proof.sha256()),
            "unverified result completion",
        )?;
        self.controls.remove(&request.0);
        Ok(())
    }

    /// Local transport failure releases only correlation capacity. The caller
    /// records delivery failure/uncertainty; it must not infer work cancellation.
    pub fn abort(&mut self, request: &RequestTag) -> Result<(), Error> {
        request.check()?;
        let removed = match request {
            RequestTag::Control { request } => self.controls.remove(&request.0).is_some(),
            RequestTag::Input { stream } => self.inputs.remove(&stream.0).is_some(),
        };
        require(removed, "unknown request abort")
    }
}

fn request_id(control: &Control) -> Option<Id> {
    Some(match control {
        Control::Session(s) => match s {
            Session::Create { request, .. }
            | Session::Binding { request, .. }
            | Session::Attach { request, .. }
            | Session::NextSequence { request }
            | Session::Sequence { request, .. } => *request,
        },
        Control::Scope(s) => match s {
            Scope::Declare { request, .. }
            | Scope::Declared { request, .. }
            | Scope::Page { request, .. }
            | Scope::PageResponse { request, .. }
            | Scope::Checkpoint { request, .. }
            | Scope::CheckpointResponse { request, .. }
            | Scope::Cancel { request, .. }
            | Scope::Cancelled { request, .. } => *request,
        },
        Control::Work(w) => match w {
            Work::Admitted { .. } => return None,
            Work::Operation { request, .. }
            | Work::OperationResponse { request, .. }
            | Work::Watch { request, .. }
            | Work::View { request, .. }
            | Work::Retry { request, .. }
            | Work::Retried { request, .. }
            | Work::Cancel { request, .. }
            | Work::Cancelled { request, .. }
            | Work::Skip { request, .. }
            | Work::Skipped { request, .. } => *request,
        },
        Control::Result(r) => match r {
            ResultMessage::Read { request, .. }
            | ResultMessage::GetManifest { request, .. }
            | ResultMessage::ManifestResponse { request, .. } => *request,
        },
        Control::Drain(d) => match d {
            Drain::Complete { request, .. }
            | Drain::Completed { request, .. }
            | Drain::Detach { request }
            | Drain::Detached { request } => *request,
        },
        Control::Refusal(Refusal {
            request: RequestTag::Control { request },
            ..
        }) => *request,
        _ => return None,
    })
}

fn validate_reply(request: &Control, response: &Control) -> Result<(), Error> {
    if matches!(response, Control::Refusal(_)) {
        return Ok(());
    }
    let matches = match (request, response) {
        (
            Control::Session(Session::Create {
                creation_sequence,
                policy,
                ..
            }),
            Control::Session(Session::Binding {
                creation_sequence: actual,
                policy: bound,
                ..
            }),
        ) => creation_sequence == actual && policy == bound,
        (
            Control::Session(Session::Attach {
                authority,
                owner,
                generation,
                ..
            }),
            Control::Session(Session::Binding {
                authority: a,
                owner: o,
                generation: g,
                ..
            }),
        ) => authority == a && owner == o && generation == g,
        (
            Control::Session(Session::NextSequence { .. }),
            Control::Session(Session::Sequence { .. }),
        ) => true,
        (
            Control::Scope(Scope::Declare {
                operation,
                scope,
                entity_ids,
                seal,
                ..
            }),
            Control::Scope(Scope::Declared { receipt, .. }),
        ) => {
            receipt.operation == *operation
                && matches!(&receipt.body, Outcome::Declared { scope: s, accepted_count, seal: digest, .. }
                if s == scope && accepted_count.0 == entity_ids.len() as u64 && digest.is_some() == *seal)
        }
        (
            Control::Scope(Scope::Page {
                scope,
                after_entity,
                limit,
                ..
            }),
            Control::Scope(Scope::PageResponse {
                scope: s, entries, ..
            }),
        ) => {
            scope == s
                && entries.len() as u64 <= limit.0
                && entries.iter().all(|e| e.entity.0 > after_entity.0)
        }
        (
            Control::Scope(Scope::Checkpoint { scope, seal, .. }),
            Control::Scope(Scope::CheckpointResponse { summary, .. }),
        ) => summary.scope == *scope && summary.seal == *seal,
        (
            Control::Scope(Scope::Cancel {
                operation, scope, ..
            }),
            Control::Scope(Scope::Cancelled { receipt, .. }),
        ) => {
            receipt.operation == *operation
                && matches!(&receipt.body, Outcome::ScopeCancelled { scope: s, .. } if s == scope)
        }
        (
            Control::Work(Work::Operation { operation, .. }),
            Control::Work(Work::OperationResponse { receipt, .. }),
        ) => receipt.operation == *operation,
        (
            Control::Work(Work::Watch {
                work,
                after_revision,
                ..
            }),
            Control::Work(Work::View {
                work: view,
                revision,
                ..
            }),
        ) => *work == view.work && revision.0 >= after_revision.0,
        (
            Control::Work(Work::Retry {
                operation,
                work,
                expected_attempt,
                ..
            }),
            Control::Work(Work::Retried { receipt, .. }),
        ) => {
            receipt.operation == *operation
                && matches!(&receipt.body, Outcome::Retried { work: w, expected_attempt: a, .. } if w == work && a == expected_attempt)
        }
        (
            Control::Work(Work::Cancel {
                operation, work, ..
            }),
            Control::Work(Work::Cancelled { receipt, .. }),
        ) => {
            receipt.operation == *operation
                && matches!(&receipt.body, Outcome::Cancelled { work: w, .. } if w == work)
        }
        (
            Control::Work(Work::Skip {
                operation, work, ..
            }),
            Control::Work(Work::Skipped { receipt, .. }),
        ) => {
            receipt.operation == *operation
                && matches!(&receipt.body, Outcome::Skipped { work: w, .. } if w == work)
        }
        (
            Control::Result(ResultMessage::GetManifest { work, attempt, .. }),
            Control::Result(ResultMessage::ManifestResponse { manifest, .. }),
        ) => manifest.work == *work && manifest.attempt == *attempt,
        (
            Control::Drain(Drain::Complete {
                generation,
                root_summary,
                ..
            }),
            Control::Drain(Drain::Completed {
                generation: g,
                root_summary: s,
                ..
            }),
        ) => generation == g && root_summary == s,
        (Control::Drain(Drain::Detach { .. }), Control::Drain(Drain::Detached { .. })) => true,
        _ => false,
    };
    require(matches, "response kind or committed fields mismatch")
}
