//! Header validation and bounded input reception. Receiving or installing an
//! input does not admit it: the funded job/receipt transaction must still commit.

use super::{
    payload::{InstalledPayload, OutputReservation, PayloadStore, StagedPayload},
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

pub enum InputPreparation {
    Replay(OperationReceipt),
    Ready(Box<PreparedInput>),
}

/// Owns validated input and a durable output-space pin. This is preparatory
/// storage, not an admission receipt, a runnable job, or execution permission.
/// The future admission transaction must revalidate and fund its whole write set.
pub struct PreparedInput {
    input: ValidatedInput,
    outputs: OutputReservation,
}
impl PreparedInput {
    pub fn input(&self) -> &ValidatedInput {
        &self.input
    }
    pub fn outputs(&self) -> &OutputReservation {
        &self.outputs
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
        let mut connection = self.connect()?;
        let tx = connection.transaction()?;
        if let Some(receipt) =
            self.check_input(&tx, identity, header, caps, payloads, applications)?
        {
            return Ok(InputReception::Replay(receipt));
        }
        // End this read snapshot before any filesystem write or network wait.
        // The opaque staged object pins itself; it is not an admitted job.
        drop(tx);
        drop(connection);
        let stage = payloads.stage(&identity.owner, &header.parameters.input, caps, now)?;
        Ok(InputReception::Receiving(Box::new(ReceivingInput {
            identity: identity.clone(),
            header: header.clone(),
            stage,
        })))
    }

    /// Prepare output file capacity and the largest work-view slot after input
    /// validation. Filesystem installation runs outside the metadata writer;
    /// state and authorization are checked again before private funding commits.
    /// Work remains DECLARED at its original revision. No operation is accepted.
    pub fn prepare_input(
        &self,
        input: ValidatedInput,
        caps: &Capabilities,
        applications: &Applications,
    ) -> Result<InputPreparation> {
        let payloads = input.payload.store();
        let mut connection = self.connect()?;
        let tx = connection.transaction()?;
        if let Some(receipt) = self.check_input(
            &tx,
            &input.identity,
            &input.header,
            caps,
            payloads,
            applications,
        )? {
            return Ok(InputPreparation::Replay(receipt));
        }
        drop(tx);
        drop(connection);
        let outputs =
            payloads.reserve_outputs(&input.identity.owner, &input.header.parameters.outputs)?;
        let mut connection = self.connect()?;
        let mut tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(receipt) = self.check_input(
            &tx,
            &input.identity,
            &input.header,
            caps,
            payloads,
            applications,
        )? {
            return Ok(InputPreparation::Replay(receipt));
        }
        let key = &input.header.parameters.work;
        let target = records::Target {
            table: records::Table::Work,
            row: tx.query_row(
                "SELECT rowid FROM work WHERE generation=?1 AND scope=?2 AND entity=?3",
                params![
                    sql(input.identity.generation.0)?,
                    sql(key.scope.0)?,
                    sql(key.entity.0)?
                ],
                |r| r.get(0),
            )?,
        };
        let retained = records::header(&tx, target)?;
        self.trusted_now(&tx)?;
        records::grow(
            &mut tx,
            target,
            retained.revision,
            retained
                .capacity
                .max(response_capacity(input.header.parameters.outputs.count)? as usize),
            retained.credits,
        )?;
        self.authorize(&input.identity.owner, Permission::Admit)?;
        commit(tx, "prepare-input")?;
        Ok(InputPreparation::Ready(Box::new(PreparedInput {
            input,
            outputs,
        })))
    }

    fn check_input(
        &self,
        tx: &Transaction<'_>,
        identity: &SessionIdentity,
        header: &InputHeader,
        caps: &Capabilities,
        payloads: &PayloadStore,
        applications: &Applications,
    ) -> Result<Option<OperationReceipt>> {
        header.check()?;
        selected(caps)?;
        let binding = self.authorize_session(tx, identity, Permission::Admit)?;
        sessions::check_connection(tx, &binding, caps)?;
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
            scopes::operation(tx, identity.generation, Producer(0), header.operation)?
        {
            if receipt.request_digest != digest {
                return Err(protocol(
                    ErrorCode::Conflict,
                    "input operation parameters changed",
                ));
            }
            return Ok(Some(receipt));
        }
        let (_, view) =
            scopes::work(tx, identity.generation, &parameters.work).map_err(|e| match e {
                StoreError::Protocol(Error {
                    code: ErrorCode::NotFound,
                    ..
                }) => protocol(ErrorCode::Conflict, "input membership was not declared"),
                other => other,
            })?;
        scopes::unfenced(tx, identity.generation, parameters.work.scope)?;
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
        Ok(None)
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
        fn validated(&self, header: &InputHeader) -> ValidatedInput {
            let now = Instant::now();
            let InputReception::Receiving(mut reception) = self.receive(header, now).unwrap()
            else {
                panic!("unexpected replay");
            };
            reception.receive(b"abc", now).unwrap();
            reception.finish(now).unwrap()
        }
        fn slot(&self) -> records::Target {
            records::Target {
                table: records::Table::Work,
                row: self
                    .authority
                    .store
                    .connect()
                    .unwrap()
                    .query_row(
                        "SELECT rowid FROM work WHERE generation=?1 AND scope=0 AND entity=1",
                        [sql(self.binding.identity.generation.0).unwrap()],
                        |r| r.get(0),
                    )
                    .unwrap(),
            }
        }
        fn assert_unadmitted(&self, header: &InputHeader) {
            let (revision, view) = self
                .authority
                .store
                .work_view(&self.binding.identity, &header.parameters.work, Number(0))
                .unwrap();
            assert_eq!(
                (revision, view.state, view.attempt),
                (Id(1), State::DECLARED, Number(0))
            );
            assert!(view.input.is_none() && view.manifest.is_none() && view.child.is_none());
            refuse(
                self.authority
                    .store
                    .operation(&self.binding.identity, header.operation),
                ErrorCode::NotFound,
            );
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
        super::super::tests::set_scope_fence(
            &fixture.authority.store,
            fixture.binding.identity.generation,
            false,
        );
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

    #[test]
    fn preparation_funds_output_and_view_capacity_without_accepting_work() {
        let fixture = Fixture::new();
        let mut header = fixture.header();
        header.parameters.outputs = OutputBudget {
            count: BatchCount(2),
            total_bytes: Number(65536),
        };
        for _ in 0..2 {
            let input = fixture.validated(&header);
            let InputPreparation::Ready(prepared) = fixture
                .authority
                .store
                .prepare_input(input, &fixture.caps(), &fixture.applications)
                .unwrap()
            else {
                panic!("unexpected replay");
            };
            assert_eq!(prepared.input().header(), &header);
            assert_eq!(prepared.outputs().budget(), &header.parameters.outputs);
            assert_eq!(prepared.outputs().owner(), &fixture.binding.identity.owner);
            assert_eq!(fixture.payloads.usage(None).unwrap().objects, 4); // input, reservation, two promised outputs
            let connection = fixture.authority.store.connect().unwrap();
            let slot = records::header(&connection, fixture.slot()).unwrap();
            assert_eq!(
                slot.capacity as u64,
                response_capacity(BatchCount(2)).unwrap()
            );
            assert_eq!(slot.credits, records::WORK_CREDITS);
            fixture.assert_unadmitted(&header);
            assert_eq!(
                fixture
                    .authority
                    .store
                    .collect_payload_orphans(&fixture.payloads, None, 256)
                    .unwrap()
                    .removed,
                0
            );
            drop(prepared);
            for _ in 0..2 {
                fixture
                    .authority
                    .store
                    .collect_payload_orphans(&fixture.payloads, None, 256)
                    .unwrap();
            }
            assert_eq!(fixture.payloads.usage(None).unwrap().objects, 0);
        }
        fixture.authority.store.integrity_check().unwrap();
    }

    #[test]
    fn preparation_rechecks_application_limits_and_scope_fences_before_funding() {
        for case in ["application", "limit", "scope"] {
            let fixture = Fixture::new();
            let mut header = fixture.header();
            header.parameters.outputs = OutputBudget {
                count: BatchCount(2),
                total_bytes: Number(8),
            };
            let input = fixture.validated(&header);
            let empty = Applications::default();
            let mut caps = fixture.caps();
            let code = match case {
                "application" => ErrorCode::ApplicationUnsupported,
                "limit" => {
                    caps.control_limit = ControlLimit(4096);
                    ErrorCode::LimitExceeded
                }
                _ => {
                    super::super::tests::set_scope_fence(
                        &fixture.authority.store,
                        fixture.binding.identity.generation,
                        false,
                    );
                    ErrorCode::Cancelled
                }
            };
            refuse(
                fixture.authority.store.prepare_input(
                    input,
                    &caps,
                    if case == "application" {
                        &empty
                    } else {
                        &fixture.applications
                    },
                ),
                code,
            );
            assert_eq!(
                fixture.payloads.usage(None).unwrap().objects,
                1,
                "no output reservation for {case}"
            );
            let connection = fixture.authority.store.connect().unwrap();
            assert_eq!(
                records::header(&connection, fixture.slot())
                    .unwrap()
                    .capacity,
                records::WORK_CAPACITY
            );
            fixture.assert_unadmitted(&header);
        }
    }

    #[test]
    fn preparation_binds_the_exact_payload_root_not_only_its_store_identity() {
        let fixture = Fixture::new();
        let header = fixture.header();
        let mut input = fixture.validated(&header);
        let other = PayloadStore::initialize(
            &fixture.authority.directory.path().join("different-root"),
            fixture.payloads.binding(),
            PayloadPolicy {
                objects: Id(8),
                bytes: Number(1 << 20),
                owner_objects: Id(8),
                owner_bytes: Number(1 << 20),
                chunk_bytes: Id(65536),
                handles: Id(8),
                owner_handles: Id(8),
            },
        )
        .unwrap();
        let now = Instant::now();
        let mut stage = other
            .stage(
                &fixture.binding.identity.owner,
                &header.parameters.input,
                &fixture.caps(),
                now,
            )
            .unwrap();
        stage.receive(b"abc", now).unwrap();
        // Construct mismatched opaque evidence inside the defining test module.
        // Public callers cannot replace ValidatedInput's private payload field.
        input.payload = stage.finish(now).unwrap();
        assert!(matches!(
            fixture
                .authority
                .store
                .prepare_input(input, &fixture.caps(), &fixture.applications),
            Err(StoreError::Corrupt(_))
        ));
        assert_eq!(other.usage(None).unwrap().objects, 1);
        fixture.assert_unadmitted(&header);
    }

    #[test]
    fn late_authorization_refusal_rolls_back_record_growth_and_leaves_only_orphans() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct RevokeOnCommit(AtomicUsize);
        impl Authorization for RevokeOnCommit {
            fn permits(&self, _: &IdentityLabel, permission: Permission) -> bool {
                permission != Permission::Admit || self.0.fetch_add(1, Ordering::SeqCst) < 2
            }
        }
        let mut fixture = Fixture::new();
        let mut header = fixture.header();
        header.parameters.outputs = OutputBudget {
            count: BatchCount(2),
            total_bytes: Number(8),
        };
        let input = fixture.validated(&header);
        fixture.authority.store.authorization = Arc::new(RevokeOnCommit(AtomicUsize::new(0)));
        refuse(
            fixture
                .authority
                .store
                .prepare_input(input, &fixture.caps(), &fixture.applications),
            ErrorCode::Unauthorized,
        );
        let connection = fixture.authority.store.connect().unwrap();
        assert_eq!(
            records::header(&connection, fixture.slot())
                .unwrap()
                .capacity,
            records::WORK_CAPACITY
        );
        fixture.assert_unadmitted(&header);
        assert_eq!(
            fixture.payloads.usage(None).unwrap().objects,
            4,
            "uncommitted payloads remain charged until safe collection"
        );
        for _ in 0..2 {
            fixture
                .authority
                .store
                .collect_payload_orphans(&fixture.payloads, None, 256)
                .unwrap();
        }
        assert_eq!(fixture.payloads.usage(None).unwrap().objects, 0);
    }

    #[test]
    fn preparation_under_an_unsafe_clock_does_not_create_a_work_promise() {
        struct UnsafeClock;
        impl Clock for UnsafeClock {
            fn read(&self) -> ClockReading {
                ClockReading {
                    utc_ms: Number(1000),
                    trusted: false,
                }
            }
        }
        let mut fixture = Fixture::new();
        let header = fixture.header();
        let input = fixture.validated(&header);
        fixture.authority.store.clock = Arc::new(UnsafeClock);
        refuse(
            fixture
                .authority
                .store
                .prepare_input(input, &fixture.caps(), &fixture.applications),
            ErrorCode::ClockUnsafe,
        );
        fixture.assert_unadmitted(&header);
    }

    #[test]
    fn response_reservation_covers_maximum_labels_numbers_and_256_manifest_outputs() {
        // Wire-representation sizing, not a fabricated completed authority job.
        let work = WorkKey {
            scope: Number(MAX_NUMBER - 1),
            producer: Producer(1),
            entity: Id(MAX_NUMBER),
        };
        let host = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61)
        );
        for count in [0, 1, 256] {
            let input = Input {
                length: Number(MAX_NUMBER),
                sha256: Digest([7; 32]),
                content_type: ApplicationLabel("i".repeat(128)),
            };
            let terminal = Number(MAX_NUMBER - MAX_DURATION);
            let manifest = Manifest {
                version: Literal, authority: IdentityLabel("a".repeat(128)), owner: IdentityLabel("b".repeat(128)),
                generation: Id(MAX_NUMBER), work: work.clone(), attempt: Id(MAX_NUMBER), input_sha256: input.sha256,
                committed_at: terminal, available_until: Number(MAX_NUMBER),
                outputs: (0..count).map(|index| Output {
                    index: OutputIndex(index), length: Number(MAX_NUMBER / 256), sha256: Digest([8; 32]),
                    content_type: ApplicationLabel("o".repeat(128)),
                    locator: ResultLocator(format!("pipestream://{host}:65535/v2/sessions/{MAX_NUMBER}/scopes/{}/producers/1/entities/{MAX_NUMBER}/attempts/{MAX_NUMBER}/outputs/{index}", MAX_NUMBER - 1)),
                }).collect(),
            };
            let view = WorkView {
                work: work.clone(),
                state: State::SUCCEEDED,
                attempt: Number(MAX_NUMBER),
                input: Some(input),
                admitted_at: Some(Number(terminal.0 - 1)),
                deadline: Some(Number(terminal.0 + 1)),
                terminal_at: Some(terminal),
                receipt_until: Some(Number(MAX_NUMBER)),
                output_until: Some(Number(MAX_NUMBER)),
                child: Some(ChildScope {
                    scope: Id(MAX_NUMBER),
                    producer: Producer(1),
                }),
                manifest: Some(manifest),
                diagnostic: Some(Diagnostic {
                    code: DiagnosticCode(u32::MAX as u64),
                    detail: Detail("d".repeat(512)),
                }),
            };
            let bound = response_capacity(BatchCount(count)).unwrap() as usize;
            let frame = Control::Work(Work::View {
                request: Id(MAX_NUMBER),
                revision: Id(MAX_NUMBER),
                work: Box::new(view),
            });
            assert!(frame.encode(bound.max(4096)).unwrap().len() <= bound + 5);
        }
    }
}
