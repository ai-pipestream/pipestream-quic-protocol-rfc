use super::{Error, ErrorCode, MAX_DURATION, MAX_NUMBER, ResultLocator, codec::*, require};
use minicbor::Decoder;

macro_rules! number {
    ($name:ident, $min:expr, $max:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        pub struct $name(pub u64);
        impl Wire for $name {
            fn read(d: &mut Decoder<'_>) -> Result<Self, Error> {
                let value = Self(d.u64().map_err(malformed)?);
                value.check()?;
                Ok(value)
            }
            fn write(&self, w: &mut Writer) {
                w.uint(self.0);
            }
            fn check(&self) -> Result<(), Error> {
                require(
                    ($min..=$max).contains(&self.0),
                    concat!(stringify!($name), " out of range"),
                )
            }
        }
    };
}
number!(Number, 0, MAX_NUMBER);
number!(Id, 1, MAX_NUMBER);
number!(Duration, 1, MAX_DURATION);
number!(Producer, 0, 1);
number!(State, 0, 8);
number!(Mode, 0, 2);
number!(Disposition, 0, 1);
number!(OutputIndex, 0, 255);
number!(BatchCount, 0, 256);
number!(PageLimit, 1, 256);
number!(WaitMs, 0, 30_000);
number!(DiagnosticCode, 0, u32::MAX as u64);
number!(StreamId, 0, (1u64 << 62) - 1);
number!(ProfileId, 1, 65_534);
number!(ControlLimit, 4096, 1_048_576);
number!(ConcurrencyLimit, 1, 1024);
number!(IdleMs, 1000, 300_000);
number!(LifetimeMs, 1000, 86_400_000);
number!(ResponseFlag, 0, 1);

impl State {
    pub const DECLARED: Self = Self(0);
    pub const ACTIVE: Self = Self(1);
    pub const AWAITING_RETRY: Self = Self(2);
    pub const WAITING_CHILDREN: Self = Self(3);
    pub const CANCELLING: Self = Self(4);
    pub const SUCCEEDED: Self = Self(5);
    pub const FAILED: Self = Self(6);
    pub const CANCELLED: Self = Self(7);
    pub const SKIPPED: Self = Self(8);
    pub fn is_terminal(self) -> bool {
        (5..=8).contains(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Literal<const VALUE: u64>;
impl<const VALUE: u64> Wire for Literal<VALUE> {
    fn read(d: &mut Decoder<'_>) -> Result<Self, Error> {
        require(d.u64().map_err(malformed)? == VALUE, "wrong record tag")?;
        Ok(Self)
    }
    fn write(&self, w: &mut Writer) {
        w.uint(VALUE);
    }
    fn check(&self) -> Result<(), Error> {
        Ok(())
    }
}

macro_rules! octets {
    ($name:ident, $size:literal, $nonzero:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name(pub [u8; $size]);
        impl Wire for $name {
            fn read(d: &mut Decoder<'_>) -> Result<Self, Error> {
                let value = Self(
                    d.bytes()
                        .map_err(malformed)?
                        .try_into()
                        .map_err(|_| Error::frame("wrong byte string size"))?,
                );
                value.check()?;
                Ok(value)
            }
            fn write(&self, w: &mut Writer) {
                w.bytes(&self.0);
            }
            fn check(&self) -> Result<(), Error> {
                require(!$nonzero || self.0 != [0; $size], "zero operation ID")
            }
        }
    };
}
octets!(Digest, 32, false);
octets!(OperationId, 16, true);

macro_rules! text_field {
    ($name:ident, $min:literal, $max:literal, $valid:expr) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name(pub String);
        impl Wire for $name {
            fn read(d: &mut Decoder<'_>) -> Result<Self, Error> {
                let text = d.str().map_err(malformed)?;
                require(
                    ($min..=$max).contains(&text.len()),
                    "text exceeds byte bound",
                )?;
                require(($valid)(text), concat!("invalid ", stringify!($name)))?;
                Ok(Self(text.to_owned()))
            }
            fn write(&self, w: &mut Writer) {
                w.text(&self.0);
            }
            fn check(&self) -> Result<(), Error> {
                require(
                    ($min..=$max).contains(&self.0.len()) && ($valid)(&self.0),
                    concat!("invalid ", stringify!($name)),
                )
            }
        }
    };
}
text_field!(IdentityLabel, 1, 128, |s: &str| s
    .bytes()
    .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b)));
text_field!(ApplicationLabel, 1, 128, |s: &str| s
    .bytes()
    .all(|b| (0x20..=0x7e).contains(&b)));
text_field!(Detail, 0, 512, |_: &str| true);

record!(
    Policy {
        execution_limit_ms: Duration,
        output_retention_ms: Duration,
        receipt_retention_ms: Duration
    } | _s
        | { Ok(()) }
);
record!(
    Limits {
        scopes: Id,
        entities: Id,
        operations: Id,
        retained_input_bytes: Number,
        retained_output_bytes: Number,
        active_jobs: Id
    } | _s
        | { Ok(()) }
);
record!(
    WorkKey {
        scope: Number,
        producer: Producer,
        entity: Id
    } | _s
        | { Ok(()) }
);
record!(
    Input {
        length: Number,
        sha256: Digest,
        content_type: ApplicationLabel
    } | _s
        | { Ok(()) }
);
record!(
    OutputBudget {
        count: BatchCount,
        total_bytes: Number
    } | _s
        | { Ok(()) }
);
record!(
    Diagnostic {
        code: DiagnosticCode,
        detail: Detail
    } | _s
        | { Ok(()) }
);
record!(
    ChildScope {
        scope: Id,
        producer: Producer
    } | _s
        | { Ok(()) }
);
record!(
    Counts {
        success: Number,
        failure: Number,
        cancelled: Number,
        skipped: Number
    } | _s
        | { Ok(()) }
);

impl Counts {
    pub fn total(&self) -> Result<u64, Error> {
        [
            self.success.0,
            self.failure.0,
            self.cancelled.0,
            self.skipped.0,
        ]
        .into_iter()
        .try_fold(0u64, |sum, n| {
            sum.checked_add(n)
                .filter(|n| *n <= MAX_NUMBER)
                .ok_or_else(|| Error::frame("count sum overflow"))
        })
    }
}

record!(ScopeSummary { scope: Number, producer: Producer, parent: Option<WorkKey>, seal: Digest, declared: Number, counts: Counts, status_root: Digest, closed_at: Number } |s| {
    scope_identity(s.scope, s.producer, s.parent.as_ref())?;
    require(s.counts.total()? == s.declared.0, "scope count partition mismatch")
});

pub(super) fn scope_identity(
    scope: Number,
    producer: Producer,
    parent: Option<&WorkKey>,
) -> Result<(), Error> {
    require(
        if scope.0 == 0 {
            producer.0 == 0 && parent.is_none()
        } else {
            parent.is_some_and(|p| p.scope.0 < scope.0)
        },
        "scope parent/producer inconsistent",
    )
}

record!(
    AdmitParameters {
        work: WorkKey,
        input: Input,
        application: ApplicationLabel,
        mode: Mode,
        execution_ms: Duration,
        outputs: OutputBudget
    } | _s
        | { Ok(()) }
);
record!(InputHeader { kind: Literal<0>, generation: Id, operation: OperationId, parameters: AdmitParameters } |_s| { Ok(()) });
record!(ResultHeader { kind: Literal<1>, request: Id, generation: Id, work: WorkKey, attempt: Id, index: OutputIndex, length: Number, sha256: Digest } |_s| { Ok(()) });
record!(
    Output {
        index: OutputIndex,
        length: Number,
        sha256: Digest,
        content_type: ApplicationLabel,
        locator: ResultLocator
    } | _s
        | { Ok(()) }
);

record!(Manifest { version: Literal<2>, authority: IdentityLabel, owner: IdentityLabel, generation: Id, work: WorkKey, attempt: Id, input_sha256: Digest, committed_at: Number, available_until: Number, outputs: Vec<Output> } |s| {
    require(s.available_until.0 > s.committed_at.0, "nonpositive output retention")?;
    require(s.available_until.0 - s.committed_at.0 <= MAX_DURATION, "output retention exceeds maximum")?;
    let mut total = 0u64;
    for (i, output) in s.outputs.iter().enumerate() {
        require(output.index.0 == i as u64, "noncontiguous output index")?;
        let target = output.locator.target()?;
        require(target.generation == s.generation && target.work == s.work && target.attempt == s.attempt && target.index == output.index, "locator/manifest identity mismatch")?;
        total = total.checked_add(output.length.0).filter(|n| *n <= MAX_NUMBER)
            .ok_or_else(|| Error::frame("output aggregate overflow"))?;
    }
    Ok(())
});

record!(WorkView { work: WorkKey, state: State, attempt: Number, input: Option<Input>, admitted_at: Option<Number>, deadline: Option<Number>, terminal_at: Option<Number>, receipt_until: Option<Number>, output_until: Option<Number>, child: Option<ChildScope>, manifest: Option<Manifest>, diagnostic: Option<Diagnostic> } |s| {
    if let Some(admitted) = s.admitted_at {
        let deadline = s.deadline.ok_or_else(|| Error::frame("admitted work lacks deadline"))?;
        require(s.input.is_some() && s.attempt.0 > 0 && s.state != State::DECLARED, "admitted work lacks input/attempt")?;
        require(deadline.0 > admitted.0 && deadline.0 - admitted.0 <= MAX_DURATION, "invalid execution interval")?;
    } else {
        require(s.input.is_none() && s.deadline.is_none() && s.child.is_none() && s.attempt.0 == 0
            && matches!(s.state, State::DECLARED | State::CANCELLING | State::CANCELLED | State::SKIPPED), "invalid inputless work")?;
    }
    if let Some(child) = &s.child {
        require(child.scope.0 > s.work.scope.0, "child scope is not newer than parent")?;
    }
    if s.state == State::WAITING_CHILDREN {
        require(s.child.is_some(), "waiting for children without child scope")?;
    }
    if s.state.is_terminal() {
        let terminal = s.terminal_at.ok_or_else(|| Error::frame("terminal time absent"))?;
        let receipt = s.receipt_until.ok_or_else(|| Error::frame("receipt expiry absent"))?;
        require(receipt.0 > terminal.0 && receipt.0 - terminal.0 <= MAX_DURATION, "invalid receipt retention")?;
        require(s.admitted_at.is_none_or(|a| a.0 <= terminal.0), "terminal precedes admission")?;
    } else {
        require(s.terminal_at.is_none() && s.receipt_until.is_none() && s.output_until.is_none() && s.manifest.is_none(), "nonterminal work has terminal evidence")?;
    }
    if matches!(s.state, State::FAILED | State::AWAITING_RETRY) {
        require(s.diagnostic.is_some(), "failure diagnostic absent")?;
    }
    if s.state != State::SUCCEEDED {
        require(s.manifest.is_none() && s.output_until.is_none(), "nonsuccess manifest")?;
    } else {
        require(s.input.is_some(), "success without input")?;
        require(s.manifest.is_some() == s.output_until.is_some(), "partial success manifest")?;
        if let Some(manifest) = &s.manifest {
            require(manifest.work == s.work && manifest.attempt.0 == s.attempt.0
                && Some(manifest.committed_at) == s.terminal_at
                && Some(manifest.available_until) == s.output_until
                && s.input.as_ref().is_some_and(|i| i.sha256 == manifest.input_sha256), "manifest disagrees with work view")?;
        }
    }
    Ok(())
});

impl WorkView {
    /// Check the selected-profile-dependent success shape in addition to the
    /// codec's context-free checks. This does not validate retained identity.
    pub fn validate_profiles(&self, results_selected: bool) -> Result<(), Error> {
        self.check()?;
        require(
            self.state != State::SUCCEEDED || self.manifest.is_some() == results_selected,
            "success disagrees with selected result profile",
        )
    }
}

message!(RequestTag {
    0 => Control { request: Id },
    1 => Input { stream: StreamId },
});
impl RequestTag {
    fn check_fields(&self) -> Result<(), Error> {
        match self {
            Self::Input { stream } => require(
                stream.0 % 4 == 2,
                "input tag must name client unidirectional stream",
            ),
            Self::Control { .. } => Ok(()),
        }
    }
}

message!(Outcome {
    0 => Admitted { work: WorkKey, attempt: Id, admitted_at: Number, deadline: Number, child: Option<ChildScope> },
    1 => Declared { scope: Number, producer: Producer, accepted_count: BatchCount, declared: Number, seal: Option<Digest> },
    2 => Retried { work: WorkKey, expected_attempt: Id, replacement_attempt: Id, accepted_at: Number },
    3 => Cancelled { work: WorkKey, accepted_at: Number, disposition: Disposition, state_at_commit: State },
    4 => ScopeCancelled { scope: Number, accepted_at: Number },
    5 => Skipped { work: WorkKey, accepted_at: Number, disposition: Disposition, state_at_commit: State },
});

impl Outcome {
    pub fn kind(&self) -> u8 {
        match self {
            Self::Admitted { .. } => 0,
            Self::Declared { .. } => 1,
            Self::Retried { .. } => 2,
            Self::Cancelled { .. } => 3,
            Self::ScopeCancelled { .. } => 4,
            Self::Skipped { .. } => 5,
        }
    }
    fn check_fields(&self) -> Result<(), Error> {
        match self {
            Self::Admitted {
                work,
                attempt,
                admitted_at,
                deadline,
                child,
            } => {
                require(attempt.0 == 1, "admission must allocate attempt one")?;
                require(
                    deadline.0 > admitted_at.0 && deadline.0 - admitted_at.0 <= MAX_DURATION,
                    "invalid admission interval",
                )?;
                require(
                    child.as_ref().is_none_or(|c| c.scope.0 > work.scope.0),
                    "invalid child scope",
                )
            }
            Self::Declared {
                scope,
                producer,
                accepted_count,
                declared,
                seal,
            } => {
                require(scope.0 != 0 || producer.0 == 0, "root producer mismatch")?;
                require(
                    accepted_count.0 <= declared.0 && (accepted_count.0 != 0 || seal.is_some()),
                    "invalid declaration receipt count",
                )
            }
            Self::Retried {
                expected_attempt,
                replacement_attempt,
                ..
            } => require(
                expected_attempt.0.checked_add(1) == Some(replacement_attempt.0),
                "retry must increment attempt once",
            ),
            Self::Cancelled {
                disposition,
                state_at_commit,
                ..
            }
            | Self::Skipped {
                disposition,
                state_at_commit,
                ..
            } => {
                let terminal = if matches!(self, Self::Cancelled { .. }) {
                    State::CANCELLED
                } else {
                    State::SKIPPED
                };
                require(
                    if disposition.0 == 0 {
                        *state_at_commit == State::CANCELLING || *state_at_commit == terminal
                    } else {
                        state_at_commit.is_terminal()
                    },
                    "invalid fence disposition/state",
                )
            }
            Self::ScopeCancelled { .. } => Ok(()),
        }
    }
}

record!(
    OperationReceipt {
        operation: OperationId,
        request_digest: Digest,
        body: Outcome
    } | _s
        | { Ok(()) }
);
record!(
    ScopeEntry {
        entity: Id,
        state: State
    } | _s
        | { Ok(()) }
);
record!(
    Refusal {
        request: RequestTag,
        code: ErrorCode,
        detail: Detail
    } | _s
        | { Ok(()) }
);

impl Wire for ErrorCode {
    fn read(d: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(match d.u64().map_err(malformed)? {
            1 => Self::FrameError,
            2 => Self::ExtensionUnsupported,
            3 => Self::Unauthorized,
            4 => Self::LimitExceeded,
            5 => Self::NotFound,
            6 => Self::Expired,
            7 => Self::Conflict,
            8 => Self::IntegrityError,
            9 => Self::NotReady,
            10 => Self::WaitTimeout,
            11 => Self::DeadlineExceeded,
            12 => Self::Cancelled,
            13 => Self::ApplicationUnsupported,
            14 => Self::ControlReset,
            15 => Self::InternalError,
            16 => Self::OutputUnavailable,
            17 => Self::ClockUnsafe,
            18 => Self::AlreadyTerminal,
            _ => return Err(Error::frame("unknown refusal code")),
        })
    }
    fn write(&self, w: &mut Writer) {
        w.uint(*self as u64);
    }
    fn check(&self) -> Result<(), Error> {
        Ok(())
    }
}

macro_rules! record_api {
    ($name:ident, $limit:expr) => {
        impl $name {
            /// Decode one bare, bounded deterministic CBOR record.
            pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
                decode(bytes, $limit)
            }
            pub fn encode(&self) -> Result<Vec<u8>, Error> {
                encode(self, $limit)
            }
        }
    };
}
record_api!(Manifest, super::MAX_CONTROL_LIMIT);
record_api!(ScopeSummary, super::MAX_CONTROL_LIMIT);
record_api!(InputHeader, super::MAX_HEADER);
record_api!(ResultHeader, super::MAX_HEADER);
