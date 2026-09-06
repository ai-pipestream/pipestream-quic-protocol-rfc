use super::*;

pub(super) fn load(
    tx: &Transaction<'_>,
    authority: &IdentityLabel,
    generation: Id,
) -> Result<Option<(Binding, bool)>> {
    let mut statement = tx.prepare("SELECT owner,creation_sequence,policy,limits,results,control_limit,object_limit,revoked FROM sessions WHERE generation=?1")?;
    let mut rows = statement.query([sql(generation.0)?])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    Ok(Some((
        Binding {
            identity: SessionIdentity {
                authority: authority.clone(),
                owner: IdentityLabel(row.get(0)?),
                generation,
            },
            creation_sequence: Id(number(row, 1)?),
            policy: unpack(&row.get::<_, Vec<u8>>(2)?)?,
            limits: unpack(&row.get::<_, Vec<u8>>(3)?)?,
            results: row.get(4)?,
            control_limit: ControlLimit(number(row, 5)?),
            object_limit: Number(number(row, 6)?),
        },
        row.get(7)?,
    )))
}

pub(super) fn check_connection(
    tx: &Transaction<'_>,
    binding: &Binding,
    caps: &Capabilities,
) -> Result<()> {
    if caps.has(RESULT_DELIVERY) != binding.results {
        return Err(protocol(
            ErrorCode::ExtensionUnsupported,
            "session profile combination changed",
        ));
    }
    let (control, object): (u64, u64) = tx.query_row(
        "SELECT required_control,required_object FROM sessions WHERE generation=?1",
        [sql(binding.identity.generation.0)?],
        |r| Ok((number(r, 0)?, number(r, 1)?)),
    )?;
    if caps.control_limit.0 < control || caps.object_limit.0 < object {
        return Err(protocol(
            ErrorCode::LimitExceeded,
            "connection cannot represent retained session promises",
        ));
    }
    Ok(())
}

impl AuthorityStore {
    /// Read-only sequence discovery does not allocate owner history or require
    /// a safe clock. The next creation still serializes against other creators.
    pub fn next_creation(&self, owner: &IdentityLabel) -> Result<Id> {
        let mut connection = self.connect()?;
        let tx = connection.transaction()?;
        self.authorize(owner, Permission::Inspect)?;
        let previous: Option<u64> = tx
            .query_row(
                "SELECT last_creation FROM owners WHERE owner=?1",
                [&owner.0],
                |r| number(r, 0),
            )
            .optional()?;
        Ok(Id(increment(previous.unwrap_or(0))?))
    }

    /// Commit owner high-water, authority generation, binding and root together.
    /// An identical replay is a retained read, including under an unsafe clock.
    pub fn create_session(
        &self,
        owner: &IdentityLabel,
        sequence: Id,
        policy: &Policy,
        caps: &Capabilities,
    ) -> Result<Binding> {
        selected(caps)?;
        sequence.check()?;
        policy.check()?;
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.authorize(owner, Permission::Create)?;
        let previous: Option<u64> = tx
            .query_row(
                "SELECT last_creation FROM owners WHERE owner=?1",
                [&owner.0],
                |r| number(r, 0),
            )
            .optional()?;
        if sequence.0 <= previous.unwrap_or(0) {
            let generation: Option<u64> = tx
                .query_row(
                    "SELECT generation FROM sessions WHERE owner=?1 AND creation_sequence=?2",
                    params![owner.0, sql(sequence.0)?],
                    |r| number(r, 0),
                )
                .optional()?;
            let Some(generation) = generation else {
                return Err(protocol(ErrorCode::Expired, "creation receipt retired"));
            };
            let (binding, revoked) = load(&tx, &self.authority, Id(generation))?
                .ok_or(StoreError::Corrupt("creation receipt lost its session"))?;
            if revoked {
                return Err(protocol(ErrorCode::Unauthorized, "authority access denied"));
            }
            check_connection(&tx, &binding, caps)?;
            if &binding.policy != policy {
                return Err(protocol(ErrorCode::Conflict, "creation policy changed"));
            }
            return Ok(binding);
        }
        if sequence.0 != increment(previous.unwrap_or(0))? {
            return Err(protocol(
                ErrorCode::Conflict,
                "creation sequence is ahead of authority",
            ));
        }
        let owner_sessions: u64 = tx.query_row(
            "SELECT count(*) FROM sessions WHERE owner=?1",
            [&owner.0],
            |r| number(r, 0),
        )?;
        let sessions: u64 = tx.query_row("SELECT count(*) FROM sessions", [], |r| number(r, 0))?;
        if owner_sessions >= self.policy.sessions_per_owner.0 || sessions >= self.policy.sessions.0
        {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "session capacity exhausted",
            ));
        }
        if previous.is_none() {
            let owners: u64 = tx.query_row("SELECT count(*) FROM owners", [], |r| number(r, 0))?;
            if owners >= self.policy.owners.0 {
                return Err(protocol(
                    ErrorCode::LimitExceeded,
                    "owner history capacity exhausted",
                ));
            }
        }
        let last: u64 = tx.query_row(
            "SELECT last_generation FROM authority WHERE singleton=1",
            [],
            |r| number(r, 0),
        )?;
        let generation = Id(increment(last)?);
        self.trusted_now(&tx)?;
        tx.execute("INSERT INTO owners(owner,last_creation) VALUES(?1,?2) ON CONFLICT(owner) DO UPDATE SET last_creation=excluded.last_creation",
            params![owner.0, sql(sequence.0)?])?;
        tx.execute(
            "UPDATE authority SET last_generation=?1 WHERE singleton=1",
            [sql(generation.0)?],
        )?;
        tx.execute("INSERT INTO sessions(generation,owner,creation_sequence,policy,limits,results,control_limit,object_limit) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![sql(generation.0)?, owner.0, sql(sequence.0)?, pack(policy)?, pack(&self.policy.session_limits)?,
                caps.has(RESULT_DELIVERY), sql(caps.control_limit.0)?, sql(caps.object_limit.0)?])?;
        records::protect(&tx, records::SUMMARY_CAPACITY, 1)?;
        tx.execute(
            "INSERT INTO scopes(generation,scope,producer,summary) VALUES(?1,0,0,zeroblob(?2))",
            params![
                sql(generation.0)?,
                (records::HEADER_BYTES + records::SUMMARY_CAPACITY) as i64
            ],
        )?;
        records::initialize(
            &tx,
            records::Target {
                table: records::Table::Scope,
                row: tx.last_insert_rowid(),
            },
            &None::<ScopeSummary>,
            records::SUMMARY_CAPACITY,
            1,
        )?;
        let binding = Binding {
            identity: SessionIdentity {
                authority: self.authority.clone(),
                owner: owner.clone(),
                generation,
            },
            creation_sequence: sequence,
            policy: policy.clone(),
            limits: self.policy.session_limits.clone(),
            results: caps.has(RESULT_DELIVERY),
            control_limit: caps.control_limit,
            object_limit: caps.object_limit,
        };
        binding
            .response(Id(MAX_NUMBER))
            .encode(caps.control_limit.0 as usize)?;
        self.authorize(owner, Permission::Create)?;
        commit(tx, "create")?;
        Ok(binding)
    }

    /// `authenticated_owner` comes from the verified connection, never its
    /// caller-supplied Attach body. Check it before reading retained identity.
    pub fn attach_session(
        &self,
        authenticated_owner: &IdentityLabel,
        expected: &SessionIdentity,
        caps: &Capabilities,
    ) -> Result<Binding> {
        selected(caps)?;
        let mut connection = self.connect()?;
        let tx = connection.transaction()?;
        self.authorize(authenticated_owner, Permission::Inspect)?;
        if authenticated_owner != &expected.owner {
            return Err(protocol(ErrorCode::Unauthorized, "authority access denied"));
        }
        let binding = self.authorize_session(&tx, expected, Permission::Inspect)?;
        check_connection(&tx, &binding, caps)?;
        Ok(binding)
    }
}
