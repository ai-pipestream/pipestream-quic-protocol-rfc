//! Header validation and bounded input reception. Receiving or installing an
//! input does not admit it: the funded job/receipt transaction must still commit.

use super::{
    payload::{InstalledPayload, PayloadStore, StagedPayload},
    *,
};
use std::collections::BTreeMap;
use std::time::Instant;

/// The configured application must actually provide these restart semantics.
/// This classification is not an exactly-once claim about external effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartSafety {
    Pure,
    IdempotentEffects,
    ExternallyFenced,
}

#[derive(Default)]
pub struct Applications {
    contracts: BTreeMap<String, (Vec<Mode>, RestartSafety)>,
}
impl Applications {
    pub fn register(
        &mut self,
        name: ApplicationLabel,
        modes: Vec<Mode>,
        safety: RestartSafety,
    ) -> Result<()> {
        name.check()?;
        require(
            !modes.is_empty()
                && modes.len() <= 3
                && modes.iter().all(|m| m.0 <= 2)
                && modes.windows(2).all(|p| p[0].0 < p[1].0),
            "invalid application modes",
        )?;
        if self.contracts.len() >= 1024 {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "application registry full",
            ));
        }
        if self.contracts.contains_key(&name.0) {
            return Err(protocol(
                ErrorCode::Conflict,
                "application contract already registered",
            ));
        }
        self.contracts.insert(name.0, (modes, safety));
        Ok(())
    }
    fn require(&self, label: &ApplicationLabel, mode: Mode) -> Result<()> {
        if !self
            .contracts
            .get(&label.0)
            .is_some_and(|(modes, _)| modes.contains(&mode))
        {
            return Err(protocol(
                ErrorCode::ApplicationUnsupported,
                "application or execution mode not configured",
            ));
        }
        Ok(())
    }
}

pub enum InputReception {
    Replay(OperationReceipt),
    Receiving(Box<ReceivingInput>),
}
pub struct ReceivingInput {
    header: InputHeader,
    identity: SessionIdentity,
    stage: StagedPayload,
}
pub struct ValidatedInput {
    header: InputHeader,
    identity: SessionIdentity,
    payload: InstalledPayload,
}
impl ReceivingInput {
    pub fn receive(&mut self, bytes: &[u8], now: Instant) -> Result<()> {
        self.stage.receive(bytes, now)
    }
    pub fn check_deadline(&mut self, now: Instant) -> Result<()> {
        self.stage.check_deadline(now)
    }
    pub fn finish(self, now: Instant) -> Result<ValidatedInput> {
        Ok(ValidatedInput {
            header: self.header,
            identity: self.identity,
            payload: self.stage.finish(now)?,
        })
    }
}
impl ValidatedInput {
    pub fn header(&self) -> &InputHeader {
        &self.header
    }
    pub fn identity(&self) -> &SessionIdentity {
        &self.identity
    }
    pub fn payload(&self) -> &InstalledPayload {
        &self.payload
    }
}

/// Conservative bound for the largest promised Work view/control response.
/// Each output allows 1024 locator bytes, 128 content-type bytes, SHA-256,
/// 63-bit length, index and CBOR overhead (under 1280 bytes). The 2048-byte
/// base covers the identities, input, times, child, diagnostic and envelopes.
/// The admission transaction must also reserve the corresponding durable space.
pub fn response_capacity(outputs: BatchCount) -> Result<u64> {
    outputs.check()?;
    Ok(2048 + outputs.0 * 1280)
}

impl AuthorityStore {
    /// The identity must be the connection's authenticated retained binding.
    /// A committing authority must repeat validation because reception holds no
    /// metadata transaction and concurrent cancellation/authorization may win.
    pub fn receive_input(
        &self,
        identity: &SessionIdentity,
        header: &InputHeader,
        caps: &Capabilities,
        payloads: &PayloadStore,
        applications: &Applications,
        now: Instant,
    ) -> Result<InputReception> {
        header.check()?;
        selected(caps)?;
        let mut connection = self.connect()?;
        let tx = connection.transaction()?;
        let binding = self.authorize_session(&tx, identity, Permission::Admit)?;
        sessions::check_connection(&tx, &binding, caps)?;
        let parameters = &header.parameters;
        if parameters.work.producer != Producer(0) {
            return Err(protocol(
                ErrorCode::Unauthorized,
                "external input cannot use authority producer",
            ));
        }
        if header.generation != identity.generation {
            return Err(protocol(
                ErrorCode::Conflict,
                "input session generation changed",
            ));
        }
        let digest =
            Mutation::Admit(parameters.clone()).digest(identity, Producer(0), header.operation)?;
        if let Some(receipt) =
            scopes::operation(&tx, identity.generation, Producer(0), header.operation)?
        {
            if receipt.request_digest != digest {
                return Err(protocol(
                    ErrorCode::Conflict,
                    "input operation parameters changed",
                ));
            }
            return Ok(InputReception::Replay(receipt));
        }
        let (_, view) =
            scopes::work(&tx, identity.generation, &parameters.work).map_err(|e| match e {
                StoreError::Protocol(Error {
                    code: ErrorCode::NotFound,
                    ..
                }) => protocol(ErrorCode::Conflict, "input membership was not declared"),
                other => other,
            })?;
        scopes::unfenced(&tx, identity.generation, parameters.work.scope)?;
        if matches!(
            view.state,
            State::CANCELLING | State::CANCELLED | State::SKIPPED
        ) {
            return Err(protocol(
                ErrorCode::Cancelled,
                "input target cancellation accepted",
            ));
        }
        if view.state != State::DECLARED {
            return Err(protocol(
                ErrorCode::Conflict,
                "work already admitted under another operation",
            ));
        }
        applications.require(&parameters.application, parameters.mode)?;
        if !binding.results
            && (parameters.outputs.count.0 != 0 || parameters.outputs.total_bytes.0 != 0)
        {
            return Err(protocol(
                ErrorCode::ExtensionUnsupported,
                "output budget requires result delivery",
            ));
        }
        if parameters.execution_ms.0 > binding.policy.execution_limit_ms.0
            || parameters.input.length.0 > binding.limits.retained_input_bytes.0
            || parameters.input.length.0 > binding.object_limit.0.min(caps.object_limit.0)
            || parameters.outputs.total_bytes.0 > binding.limits.retained_output_bytes.0
            || response_capacity(parameters.outputs.count)?
                > caps.control_limit.0.min(binding.control_limit.0)
        {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "input exceeds retained duration, bytes or response limits",
            ));
        }
        let (store_id, root): (Vec<u8>, Option<String>) = tx.query_row(
            "SELECT store_id,payload_path FROM authority WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if store_id != payloads.binding().as_bytes() || root.as_deref() != payloads.path().to_str()
        {
            return Err(StoreError::Corrupt(
                "input payload root is not bound to authority",
            ));
        }
        // End this read snapshot before any filesystem write or network wait.
        // The opaque staged object pins itself; it is not an admitted job.
        drop(tx);
        drop(connection);
        let stage = payloads.stage(&identity.owner, &parameters.input, caps, now)?;
        Ok(InputReception::Receiving(Box::new(ReceivingInput {
            identity: identity.clone(),
            header: header.clone(),
            stage,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::super::payload::PayloadPolicy;
    use super::*;
    use sha2::{Digest as _, Sha256};

    struct Fixture {
        authority: super::super::tests::Fixture,
        binding: Binding,
        payloads: PayloadStore,
        applications: Applications,
    }
    impl Fixture {
        fn new() -> Self {
            let authority = super::super::tests::Fixture::new();
            let binding = authority.create();
            authority
                .store
                .declare(
                    &binding.identity,
                    OperationId([1; 16]),
                    Number(0),
                    &[Id(1), Id(2)],
                    false,
                )
                .unwrap();
            let payloads = PayloadStore::initialize(
                &authority.directory.path().join("objects"),
                authority.store.payload_identity().unwrap(),
                PayloadPolicy {
                    objects: Id(16),
                    bytes: Number(4 << 20),
                    owner_objects: Id(8),
                    owner_bytes: Number(2 << 20),
                    chunk_bytes: Id(65536),
                    handles: Id(16),
                    owner_handles: Id(8),
                },
            )
            .unwrap();
            authority.store.bind_payloads(&payloads).unwrap();
            let mut applications = Applications::default();
            applications
                .register(
                    ApplicationLabel("uppercase/v1".into()),
                    vec![Mode(0), Mode(1), Mode(2)],
                    RestartSafety::Pure,
                )
                .unwrap();
            Self {
                authority,
                binding,
                payloads,
                applications,
            }
        }
        fn caps(&self) -> Capabilities {
            Capabilities {
                response: ResponseFlag(1),
                supported: vec![
                    ProfileId(DURABLE_WORK.into()),
                    ProfileId(RESULT_DELIVERY.into()),
                ],
                required: vec![],
                control_limit: self.binding.control_limit,
                stream_limit: ConcurrencyLimit(8),
                pending_limit: ConcurrencyLimit(8),
                object_limit: self.binding.object_limit,
                stream_idle_ms: IdleMs(1000),
                stream_lifetime_ms: LifetimeMs(10000),
            }
        }
        fn header(&self) -> InputHeader {
            InputHeader {
                kind: Literal,
                generation: self.binding.identity.generation,
                operation: OperationId([2; 16]),
                parameters: AdmitParameters {
                    work: WorkKey {
                        scope: Number(0),
                        producer: Producer(0),
                        entity: Id(1),
                    },
                    input: Input {
                        length: Number(3),
                        sha256: Digest(Sha256::digest(b"abc").into()),
                        content_type: ApplicationLabel("application/octet-stream".into()),
                    },
                    application: ApplicationLabel("uppercase/v1".into()),
                    mode: Mode(0),
                    execution_ms: Duration(1000),
                    outputs: OutputBudget {
                        count: BatchCount(0),
                        total_bytes: Number(0),
                    },
                },
            }
        }
        fn receive(&self, header: &InputHeader, now: Instant) -> Result<InputReception> {
            self.authority.store.receive_input(
                &self.binding.identity,
                header,
                &self.caps(),
                &self.payloads,
                &self.applications,
                now,
            )
        }
    }
    fn refuse<T>(result: Result<T>, expected: ErrorCode) {
        match result {
            Err(StoreError::Protocol(error)) => assert_eq!(error.code, expected),
            Err(other) => panic!("unexpected {other:?}"),
            Ok(_) => panic!("expected {expected:?}"),
        }
    }

    #[test]
    fn validated_input_is_not_admission_or_processing() {
        let fixture = Fixture::new();
        let now = Instant::now();
        let header = fixture.header();
        let InputReception::Receiving(mut input) = fixture.receive(&header, now).unwrap() else {
            panic!("unexpected replay");
        };
        input.receive(b"a", now).unwrap();
        input.receive(b"bc", now).unwrap();
        let validated = input.finish(now).unwrap();
        assert_eq!(validated.header(), &header);
        assert_eq!(validated.identity(), &fixture.binding.identity);
        assert_eq!(validated.payload().descriptor(), &header.parameters.input);
        let (_, view) = fixture
            .authority
            .store
            .work_view(
                &fixture.binding.identity,
                &header.parameters.work,
                Number(0),
            )
            .unwrap();
        assert_eq!(view.state, State::DECLARED);
        assert_eq!(view.attempt, Number(0));
        assert!(view.input.is_none());
        refuse(
            fixture
                .authority
                .store
                .operation(&fixture.binding.identity, header.operation),
            ErrorCode::NotFound,
        );
        assert_eq!(
            fixture
                .authority
                .store
                .collect_payload_orphans(&fixture.payloads, None, 256)
                .unwrap()
                .removed,
            0
        );
        drop(validated);
        assert_eq!(
            fixture
                .authority
                .store
                .collect_payload_orphans(&fixture.payloads, None, 256)
                .unwrap()
                .removed,
            1
        );
    }

    #[test]
    fn header_refusals_precede_any_payload_allocation() {
        let fixture = Fixture::new();
        let now = Instant::now();
        let cases: Vec<(InputHeader, ErrorCode)> = (0..9)
            .map(|case| {
                let mut header = fixture.header();
                let code = match case {
                    0 => {
                        header.parameters.work.producer = Producer(1);
                        ErrorCode::Unauthorized
                    }
                    1 => {
                        header.generation = Id(999);
                        ErrorCode::Conflict
                    }
                    2 => {
                        header.parameters.work.entity = Id(999);
                        ErrorCode::Conflict
                    }
                    3 => {
                        header.parameters.application = ApplicationLabel("unknown/v1".into());
                        ErrorCode::ApplicationUnsupported
                    }
                    4 => {
                        header.parameters.mode = Mode(3);
                        ErrorCode::FrameError
                    }
                    5 => {
                        header.parameters.execution_ms =
                            Duration(fixture.binding.policy.execution_limit_ms.0 + 1);
                        ErrorCode::LimitExceeded
                    }
                    6 => {
                        header.parameters.input.length = Number(fixture.binding.object_limit.0 + 1);
                        ErrorCode::LimitExceeded
                    }
                    7 => {
                        header.parameters.outputs.count = BatchCount(256);
                        ErrorCode::LimitExceeded
                    }
                    _ => {
                        header.operation = OperationId([1; 16]);
                        ErrorCode::Conflict
                    }
                };
                (header, code)
            })
            .collect();
        for (header, code) in cases {
            refuse(fixture.receive(&header, now), code);
            assert_eq!(fixture.payloads.usage(None).unwrap().objects, 0);
        }
        let mut wrong = fixture.binding.identity.clone();
        wrong.owner = IdentityLabel("bob".into());
        refuse(
            fixture.authority.store.receive_input(
                &wrong,
                &fixture.header(),
                &fixture.caps(),
                &fixture.payloads,
                &fixture.applications,
                now,
            ),
            ErrorCode::Unauthorized,
        );
        fixture
            .authority
            .store
            .connect()
            .unwrap()
            .execute(
                "UPDATE scopes SET cancelled=1 WHERE generation=?1",
                [sql(fixture.binding.identity.generation.0).unwrap()],
            )
            .unwrap();
        refuse(
            fixture.receive(&fixture.header(), now),
            ErrorCode::Cancelled,
        );
        assert_eq!(fixture.payloads.usage(None).unwrap().objects, 0);
    }

    #[test]
    fn invalid_fin_preserves_declaration_and_operation_uncertainty() {
        let fixture = Fixture::new();
        let now = Instant::now();
        let header = fixture.header();
        let InputReception::Receiving(mut input) = fixture.receive(&header, now).unwrap() else {
            panic!("unexpected replay");
        };
        input.receive(b"abd", now).unwrap();
        refuse(input.finish(now), ErrorCode::IntegrityError);
        assert_eq!(fixture.payloads.usage(None).unwrap().objects, 0);
        assert_eq!(
            fixture
                .authority
                .store
                .work_view(
                    &fixture.binding.identity,
                    &header.parameters.work,
                    Number(0)
                )
                .unwrap()
                .1
                .state,
            State::DECLARED
        );
        refuse(
            fixture
                .authority
                .store
                .operation(&fixture.binding.identity, header.operation),
            ErrorCode::NotFound,
        );
    }
}
